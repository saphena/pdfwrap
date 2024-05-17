package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
	yaml "gopkg.in/yaml.v2"
)

const ProgramVersion = "PDFWrap v1.0 - Copyright (c) 2024 Bob Stammers"

//go:embed pdfwrap.yml
var mycfg string

var configPath = flag.String("cfg", "", "Configuration file")
var silent = flag.Bool("s", false, "Run silently")
var debug = flag.Bool("debug", false, "Show debugging info")

type MySQL struct {
	Server   string
	Userid   string
	Password string
	Database string
}

type PDFTK struct {
	Exec       string
	Folder     string
	PDFMask    string
	Infofile   string
	Title      string
	Author     string
	PDFPrefix  string
	PDFPrefix2 string
	PDFPrefix3 string
	OwnerPass  string
	FinalArgs  string
}

type STREAM struct {
	Rpt         string // Crystal Reports template
	Table       string // tletterqq, dd_notify
	PlanNo      string // SQL to retrieve PlanNo as string
	Ltrid       string // StdLetter number
	Blank       string // PDF of blank letterhead
	PrintedWhen string // Column name of PrintedWhen if present
}

type CRNINJA struct {
	Exec      string
	DBAccess  string
	Crletters STREAM
	Crdouble  STREAM
}

type TERMS map[string]string

type EMAIL struct {
	Bcc               string
	Subject           string
	Bodytext          string
	Terms             TERMS
	BadEmailDefault   string
	BadProductDefault string
	SendingUser       string
	PlanFields        []string
}

type DDS struct {
	Page2Ltr string
}

var CFG struct {
	MySQL   MySQL
	Pdftk   PDFTK
	Email   EMAIL
	DDs     DDS
	Crninja CRNINJA
}

// Flag used on database to indicate letter sent via email rather than paper
const DELMETH_EMAIL = "1"

var DBH *sql.DB

func main() {

	var err error

	flag.Parse()

	if !*silent {
		fmt.Println(ProgramVersion)
	}
	loadConfig()

	if *debug {
		fmt.Println("Opening database " + CFG.MySQL.Server)
	}
	connectStr := CFG.MySQL.Userid + ":" + CFG.MySQL.Password + "@tcp(" + CFG.MySQL.Server + ")/" + CFG.MySQL.Database
	//connectStr += "?allowCleartextPasswords=true"
	DBH, err = sql.Open("mysql", connectStr)
	checkerr(err)
	defer DBH.Close()
	if !checkDatabase() {
		os.Exit(1)
	}
	if *debug {
		fmt.Println("Database opened")
	}

	processLetterQ()
	processDDQ()
	makeSecurePDFs()
	if !*silent {
		fmt.Println("Run complete")
	}
}

// Alphabetic below

func checkDatabase() bool {

	rows, err := DBH.Query("SELECT Count(*) FROM tliterals")
	checkerr(err)
	defer rows.Close()
	var res int64
	if rows.Next() {
		rows.Scan(&res)
		if *debug {
			fmt.Printf("Count(tliterals)=%v\n", res)
		}
	}
	return true
}

func checkerr(err error) {

	if err != nil {
		panic(err.Error())
	}

}

func emailSecurePDF(pdf string, plandata []string) {
	//    0       1      2       3        4        5         6             7             8          9
	// Product,cEmail,cPhone,cPostcode,cTitle,cFirstname,cLastname,CustomerPassword,RecordStatus,PlanNo

	DearSir := plandata[4]
	if DearSir == "" {
		DearSir = plandata[5][:1] // First initial
	}
	DearSir += " " + plandata[6]
	BodyText := CFG.Email.Bodytext
	BodyText = strings.ReplaceAll(BodyText, "#DearSir#", DearSir)
	for pi, pf := range CFG.Email.PlanFields {
		BodyText = strings.ReplaceAll(BodyText, "#"+pf+"#", plandata[pi])
	}

	xsql := "INSERT INTO toutgoingemails (SentAt,SentBy,PlanNo,ToAddress"
	if CFG.Email.Bcc == "" {
		xsql += ",BCAddress"
	}
	xsql += ",Subject,MsgText,Attachments) VALUES("
	xsql += "Now(),'" + safesql(CFG.Email.SendingUser) + "'," + safesql(plandata[9])
	if plandata[1] == "" {
		plandata[1] = safesql(CFG.Email.BadEmailDefault)
	}
	xsql += ",'" + safesql(plandata[1]) + "'"
	if CFG.Email.Bcc == "" {
		xsql += ",'" + safesql(CFG.Email.Bcc) + "'"
	}
	xsql += ",'" + safesql(CFG.Email.Subject) + "'"
	xsql += ",'" + safesql(BodyText) + "'"
	xsql += ",'" + safesql(pdf) + "'"
	xsql += ")"
	runsql(xsql)

}

func formatDate(iso8601 string) string {

	return iso8601[8:10] + "/" + iso8601[5:7] + "/" + iso8601[0:4]
}

func formatDDPage2s() {

	// This formats the relevant standard letter into each of the DD_NOTIFY records
	// ready for DD notice printing

	const FETCHTEXT = `FROM tStdLetters 
						LEFT JOIN (tStdLetterHeaders, tStdLetterFooters) 
						ON tStdLetters.LtrHeaderID=tStdLetterHeaders.HdrID 
						AND tStdLetters.LtrFooterID=tStdLetterFooters.FtrID 
						WHERE LtrID=`
	bodyText := getStringFromDB("SELECT LtrBody "+FETCHTEXT+CFG.DDs.Page2Ltr, "")
	//	headText := getStringFromDB("SELECT HdrHeader "+FETCHTEXT+CFG.DDs.Page2Ltr, "")
	//	footText := getStringFromDB("SELECT FtrFooter "+FETCHTEXT+CFG.DDs.Page2Ltr, "")
	var page2s = make(map[int]string)

	xsql := "SELECT dd_notify.ID, dd_notify.AccountRef FROM dd_notify WHERE edited=0"
	rows, err := DBH.Query((xsql))
	checkerr(err)
	defer rows.Close()
	for rows.Next() {
		var id int
		var account string
		rows.Scan(&id, &account)
		page2s[id] = account
	}
	rows.Close()
	for id, plan := range page2s {
		xsql := "UPDATE dd_notify SET ltr2Body='" + safesql(replaceFields(bodyText, plan)) + "' WHERE id=" + strconv.Itoa(id)
		runsql(xsql)
	}

}

func generatePDFs(whichq STREAM) {

	// Need to process letter queue one record at a time so ...
	// First, mark the whole batch as belonging to me

	xsql := "SELECT MAX(PrintBatch) AS MaxBatch FROM " + whichq.Table
	Batch2Print := getIntegerFromDB(xsql, 0)

	xsql = "SET @B := " + strconv.FormatInt(Batch2Print, 10) + ";"
	runsql(xsql)
	xsql = "UPDATE " + whichq.Table + " SET PrintBatch=(SELECT @B := @B + 1)"
	if whichq.PrintedWhen != "" {
		xsql += "," + whichq.PrintedWhen + "=" + sqldate(time.Now())
	}
	xsql += " WHERE PrintBatch=0 AND DelMeth=" + DELMETH_EMAIL
	runsql(xsql)
	LastBatch := getIntegerFromDB("SELECT (@B := @B + 1)", 0)

	// Now loop through that marked batch
	xsql = "SELECT " + whichq.PlanNo + "," + whichq.Ltrid + " FROM " + whichq.Table
	xsql += " WHERE PrintBatch > " + strconv.FormatInt(Batch2Print, 10) + " AND PrintBatch <= " + strconv.FormatInt(LastBatch, 10)
	if *debug {
		fmt.Println(xsql)
	}
	rows, err := DBH.Query(xsql)
	checkerr(err)
	defer rows.Close()
	ndox := 0
	for rows.Next() {
		var PlanNo string
		var Ltrid string
		rows.Scan(&PlanNo, &Ltrid)
		Batch2Print++
		ndox++
		fname := filepath.Join(CFG.Pdftk.Folder, CFG.Pdftk.PDFPrefix+PlanNo+"-"+Ltrid+"-draft.pdf")
		fname2 := strings.Replace(fname, "-draft.pdf", ".pdf", 1)

		// Now run CrystalReportsNinja to generate the PDF
		args := []string{"-F", CFG.Crninja.Crletters.Rpt, "-O", fname}
		args = append(args, "-E", "pdf")
		args = append(args, "-a", "PrintBatch:"+strconv.FormatInt(Batch2Print, 10))
		args = append(args, strings.Split(CFG.Crninja.DBAccess, " ")...)

		if *debug {
			fmt.Printf(`CRNINJA: "%v" %v`+"\n", CFG.Crninja.Exec, strings.Join(args, " "))
		}
		cmd := exec.Command(CFG.Crninja.Exec, args...)
		err := cmd.Run()
		checkerr(err)

		args = []string{fname}
		if whichq.Blank != "" {
			args = append(args, "background", filepath.Join(CFG.Pdftk.Folder, whichq.Blank))
		}
		args = append(args, "output", fname2)
		runPdftk(args)
		os.Remove(fname)

	}
	if !*silent {
		fmt.Printf("%v PDFs generated\n", ndox)
	}

}

func getFloatFromDB(xsql string, xdef float64) float64 {

	rows, err := DBH.Query(xsql)
	if err != nil {
		return xdef
	}
	defer rows.Close()
	var res float64
	if rows.Next() {
		rows.Scan(&res)
		return res
	} else {
		return xdef
	}

}
func getIntegerFromDB(xsql string, xdef int64) int64 {

	if *debug {
		fmt.Println(xsql)
	}
	rows, err := DBH.Query(xsql)
	if err != nil {
		if *debug {
			fmt.Printf("getIntegerFromDB FAILED - %v\n", err.Error())
		}
		return xdef
	}
	defer rows.Close()
	var res int64
	if rows.Next() {
		rows.Scan(&res)
		if *debug {
			fmt.Printf("Returning %v\n", res)
		}
		return res
	} else {
		return xdef
	}
}

func getStringFromDB(xsql string, xdef string) string {

	if *debug {
		fmt.Println(xsql)
	}
	rows, err := DBH.Query(xsql)
	if err != nil {
		if *debug {
			fmt.Printf("getStringFromDB FAILED - %v\n", err.Error())
			os.Exit(1)
		}

		return xdef
	}
	defer rows.Close()
	var res string
	if rows.Next() {
		rows.Scan(&res)
		if *debug {
			fmt.Printf("Returning '%v'\n", res)
		}
		return res
	} else {
		return xdef
	}

}

func loadConfig() {

	d := yaml.NewDecoder(strings.NewReader(mycfg))

	cfgp := &CFG

	if err := d.Decode(&cfgp); err != nil {
		fmt.Printf("Embedded parse failed %v\n", err)
		return
	}

	if *configPath == "" {
		return
	}
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		return
	}

	file, err := os.Open(*configPath)
	if err != nil {
		return
	}
	defer file.Close()

	if !*silent {
		fmt.Printf("Parsing %v\n", *configPath)
	}

	// Start YAML decoding from file
	d = yaml.NewDecoder(file)

	if err := d.Decode(&cfgp); err != nil {
		fmt.Printf("Parse failed %v\n", err)
		return
	}
}

func makeInfoFile() {

	/*
	 * This creates a text file in the format required by PDFTK used to hold
	 * metadata for the generated PDFs.
	 *
	 */

	const datefmt = "20060102150405000" // Equivalent to VB.Net string "yyyyMMddhhmmsszzz"

	f, err := os.Create(filepath.Join(CFG.Pdftk.Folder, CFG.Pdftk.Infofile))
	checkerr(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	w.WriteString("InfoBegin\n")
	w.WriteString("InfoKey: Title\n")
	w.WriteString("InfoValue: " + CFG.Pdftk.Title + "\n")
	w.WriteString("InfoBegin\n")
	w.WriteString("InfoKey: Author\n")
	w.WriteString("InfoValue: " + CFG.Pdftk.Author + "\n")
	w.WriteString("InfoBegin\n")
	w.WriteString("InfoKey: Producer\n")
	w.WriteString("InfoValue: " + ProgramVersion + "\n")
	w.WriteString("InfoBegin\n")
	w.WriteString("InfoKey: CreationDate\n")
	t := time.Now()
	w.WriteString("InfoValue: D'" + t.Format(datefmt) + "'\n")
	w.Flush()

}

func makeSecurePDFs() {

	const DATA_SEPARATOR = ";;"

	//    0       1      2       3        4        5         6             7             8          9
	// Product,cEmail,cPhone,cPostcode,cTitle,cFirstname,cLastname,CustomerPassword,RecordStatus,PlanNo
	var pdsql = `SELECT Concat_WS('` + DATA_SEPARATOR + `',IfNull(Product,'` + CFG.Email.BadProductDefault + `'),
					IfNull(cEmail,'` + CFG.Email.BadEmailDefault + `'),
					IfNull(cPhone,''),IfNull(cPostcode,''),
					IfNull(cTitle,''),
					IfNull(cFirstname,''),
					IfNull(cLastname,''),
					IfNull(CustomerPassword,''),
					RecordStatus,PlanNo) AS PlanData FROM tcustomers WHERE PlanNo=`

	if !*silent {
		fmt.Println("Making secure PDFs ... ")
	}

	makeInfoFile()

	x := filepath.Join(CFG.Pdftk.Folder, CFG.Pdftk.PDFPrefix+"*.pdf")
	if *debug {
		fmt.Printf("Scanning %v\n", x)
	}
	files, _ := os.ReadDir(CFG.Pdftk.Folder)
	myfile, _ := regexp.Compile(CFG.Pdftk.PDFMask)
	rplan, _ := regexp.Compile(`-(\d+)-`)
	nrex := 0
	for _, file := range files {
		Filename := file.Name()
		if !myfile.MatchString(Filename) {
			continue
		}
		nrex++
		if *debug {
			fmt.Printf("Securing %v\n", Filename)
		}
		PlanNo := rplan.FindStringSubmatch(Filename)
		if len(PlanNo) < 2 || PlanNo[1] == "" {
			if !*silent {
				fmt.Printf("Cannot process file %v. No Plan number\n", Filename)
			}
			continue
		}
		PlanData := strings.Split(getStringFromDB(pdsql+PlanNo[1], ""), DATA_SEPARATOR)

		// We're going to use the Plan's main phone number as the encryption key
		password := strings.ReplaceAll(PlanData[2], " ", "")
		tmp := filepath.Join(CFG.Pdftk.Folder, Filename)
		tm2 := filepath.Join(CFG.Pdftk.Folder, strings.Replace(Filename, CFG.Pdftk.PDFPrefix, CFG.Pdftk.PDFPrefix2, 1))
		sa := filepath.Join(CFG.Pdftk.Folder, strings.Replace(Filename, CFG.Pdftk.PDFPrefix, CFG.Pdftk.PDFPrefix3, 1))
		args := []string{tmp}
		args = append(args, CFG.Email.Terms[PlanData[0]])
		args = append(args, "output", tm2)
		runPdftk(args)

		args = []string{tm2}
		args = append(args, "update_info", filepath.Join(CFG.Pdftk.Folder, CFG.Pdftk.Infofile))
		args = append(args, "output", sa)
		args = append(args, "owner_pw", CFG.Pdftk.OwnerPass)
		args = append(args, "user_pw", password)
		runPdftk(args)

		// No longer need .tmp or .tm2
		os.Remove(filepath.Join(CFG.Pdftk.Folder, file.Name()))
		os.Remove(filepath.Join(CFG.Pdftk.Folder, strings.Replace(file.Name(), CFG.Pdftk.PDFPrefix, CFG.Pdftk.PDFPrefix2, 1)))
		emailSecurePDF(sa, PlanData)
	}
	if !*silent {
		fmt.Printf("%v PDFs secured\n", nrex)
	}

}

func processDDQ() {

	if !*silent {
		fmt.Println("Processing DDs ...")
	}
	formatDDPage2s()
	generatePDFs(CFG.Crninja.Crdouble)

}

func processLetterQ() {

	if !*silent {
		fmt.Println("Processing letters ... ")
	}
	generatePDFs(CFG.Crninja.Crletters)

}

func replaceFields(txt string, planno string) string {

	//Field types held in tStdLetterFields
	const FIELD_VALUE_TYPE_TEXT = 0
	const FIELD_VALUE_TYPE_INTEGER = 1
	const FIELD_VALUE_TYPE_CURRENCY = 2
	const FIELD_VALUE_TYPE_DATE = 3

	var res string

	res = txt
	rfldx, _ := regexp.Compile(`\[\[(\w+)\]\]`)
	rflds := rfldx.FindAllStringSubmatch(txt, -1)
	for i := 0; i < len(rflds); i++ {
		fld := safesql(rflds[i][1])
		xsql := "SELECT FieldSQL FROM tstdletterfields WHERE FieldID='" + fld + "'"
		fieldSQL := getStringFromDB(xsql, "")
		if fieldSQL == "" {
			continue
		}
		xsql = "SELECT FieldValueType FROM tstdletterfields WHERE FieldID='" + fld + "'"
		fieldType := getIntegerFromDB(xsql, FIELD_VALUE_TYPE_TEXT)

		xsql = "SELECT " + fieldSQL + "  WHERE PlanNo=" + planno
		xnew := ""

		switch fieldType {
		case FIELD_VALUE_TYPE_CURRENCY:
			xval := getFloatFromDB(xsql, 0.00)
			xnew = "£" + strconv.FormatFloat(xval, 'E', 2, 64)
		case FIELD_VALUE_TYPE_DATE:
			xval := getStringFromDB(xsql, "2004-01-01")
			xnew = formatDate(xval)
		case FIELD_VALUE_TYPE_INTEGER:
			xval := getIntegerFromDB(xsql, 0)
			xnew = strconv.FormatInt(xval, 10)
		default:
			xnew = getStringFromDB(xsql, "")
		}
		res = strings.ReplaceAll(res, "[["+fld+"]]", xnew)

	}

	return res
}

func runPdftk(args []string) {

	argx := args
	if CFG.Pdftk.FinalArgs != "" {
		argx = append(args, CFG.Pdftk.FinalArgs)
	}
	if *debug {
		fmt.Printf(`PDFTK: "%v" %v`+"\n", CFG.Pdftk.Exec, strings.Join(argx, " "))
	}
	cmd := exec.Command(CFG.Pdftk.Exec, argx...)
	err := cmd.Run()
	checkerr(err)

}

func runsql(xsql string) {

	if *debug {
		fmt.Println(xsql)
	}
	_, err := DBH.Exec(xsql)
	checkerr(err)
}

func safesql(x string) string {

	var sb strings.Builder
	for i := 0; i < len(x); i++ {
		c := x[i]
		switch c {
		case '\\', 0, '\n', '\r', '\'', '"':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '\032':
			sb.WriteByte('\\')
			sb.WriteByte('Z')
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}

func sqldate(tm time.Time) string {

	const datefmt = "2006-01-02"

	return "'" + tm.Format(datefmt) + "'"
}

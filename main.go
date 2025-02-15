// main.go
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

// --- Constantes et enums ---

var SupportedManifestSchemaVersions = []string{
	"https://schemas.getdbt.com/dbt/manifest/v4.json",
	"https://schemas.getdbt.com/dbt/manifest/v5.json",
	"https://schemas.getdbt.com/dbt/manifest/v6.json",
	"https://schemas.getdbt.com/dbt/manifest/v7.json",
	"https://schemas.getdbt.com/dbt/manifest/v8.json",
	"https://schemas.getdbt.com/dbt/manifest/v9.json",
	"https://schemas.getdbt.com/dbt/manifest/v10.json",
	"https://schemas.getdbt.com/dbt/manifest/v11.json",
	"https://schemas.getdbt.com/dbt/manifest/v12.json",
}

type CoverageType string

const (
	CoverageTypeDoc  CoverageType = "doc"
	CoverageTypeTest CoverageType = "test"
)

type CoverageFormat string

const (
	FormatStringTable   CoverageFormat = "string"
	FormatMarkdownTable CoverageFormat = "markdown"
)

// --- Structures internes ---

// Column représente la couverture d'une colonne (documentation et tests)
type Column struct {
	Name string
	Doc  bool
	Test bool
}

// Table contient les informations sur une table et ses colonnes.
type Table struct {
	UniqueID         string
	Name             string
	OriginalFilePath string
	Columns          map[string]Column
}

// Catalog contient l'ensemble des tables du catalog.
type Catalog struct {
	Tables map[string]Table
}

// Manifest représente le manifest dbt.
type Manifest struct {
	Sources   map[string]map[string]interface{}
	Models    map[string]map[string]interface{}
	Seeds     map[string]map[string]interface{}
	Snapshots map[string]map[string]interface{}
	Tests     map[string]map[string][]interface{}
}

type ColumnReport struct {
	Name     string  `json:"name"`
	Covered  int     `json:"covered"`
	Total    int     `json:"total"`
	Coverage float64 `json:"coverage"`
}

type TableReport struct {
	Name     string         `json:"name"`
	Covered  int            `json:"covered"`
	Total    int            `json:"total"`
	Coverage float64        `json:"coverage"`
	Columns  []ColumnReport `json:"columns"`
}

type JSONReport struct {
	CovType  string        `json:"cov_type"`
	Covered  int           `json:"covered"`
	Total    int           `json:"total"`
	Coverage float64       `json:"coverage"`
	Tables   []TableReport `json:"tables"`
}

func NewColumnFromNode(node map[string]interface{}) Column {
	name := strings.ToLower(node["name"].(string))
	return Column{Name: name}
}

func IsValidDoc(doc interface{}) bool {
	if doc == nil {
		return false
	}
	if s, ok := doc.(string); ok {
		return s != ""
	}
	return false
}

func IsValidTest(tests []interface{}) bool {
	return len(tests) > 0
}

func NewTableFromNode(node map[string]interface{}, manifest *Manifest) (Table, error) {
	uniqueID, ok := node["unique_id"].(string)
	if !ok {
		return Table{}, errors.New("unique_id absent ou invalide")
	}
	manifestTable, err := manifest.GetTable(uniqueID)
	if err != nil {
		return Table{}, fmt.Errorf("unique_id %s non trouvé dans le manifest", uniqueID)
	}
	cols := make(map[string]Column)
	if columnsRaw, ok := node["columns"].(map[string]interface{}); ok {
		for _, v := range columnsRaw {
			if colNode, ok := v.(map[string]interface{}); ok {
				col := NewColumnFromNode(colNode)
				cols[col.Name] = col
			}
		}
	}
	origPath := ""
	if v, ok := manifestTable["original_file_path"].(string); ok {
		origPath = v
	} else {
		log.Printf("warning: original_file_path introuvable pour %s", uniqueID)
	}
	name := strings.ToLower(manifestTable["name"].(string))
	return Table{
		UniqueID:         uniqueID,
		Name:             name,
		OriginalFilePath: origPath,
		Columns:          cols,
	}, nil
}

func (c Catalog) FilterTables(modelPathFilter []string) Catalog {
	filtered := make(map[string]Table)
	for id, table := range c.Tables {

		originalPath := filepath.ToSlash(table.OriginalFilePath)
		for _, filt := range modelPathFilter {

			normalizedFilt := filepath.ToSlash(filt)
			if strings.HasPrefix(originalPath, normalizedFilt) {
				filtered[id] = table
				break
			}
		}
	}
	log.Printf("Tables après filtrage : %d", len(filtered))
	return Catalog{Tables: filtered}
}

func CatalogFromNodes(nodes []interface{}, manifest *Manifest) (Catalog, error) {
	tables := make(map[string]Table)
	for _, n := range nodes {
		if node, ok := n.(map[string]interface{}); ok {
			table, err := NewTableFromNode(node, manifest)
			if err != nil {
				return Catalog{}, err
			}
			tables[table.UniqueID] = table
		}
	}
	return Catalog{Tables: tables}, nil
}

func (m *Manifest) GetTable(tableID string) (map[string]interface{}, error) {
	candidates := []map[string]interface{}{}
	if v, ok := m.Sources[tableID]; ok {
		candidates = append(candidates, v)
	}
	if v, ok := m.Models[tableID]; ok {
		candidates = append(candidates, v)
	}
	if v, ok := m.Seeds[tableID]; ok {
		candidates = append(candidates, v)
	}
	if v, ok := m.Snapshots[tableID]; ok {
		candidates = append(candidates, v)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("table %s non trouvée", tableID)
	}
	if len(candidates) > 1 {
		return nil, fmt.Errorf("unique_id %s en double", tableID)
	}
	return candidates[0], nil
}

func ManifestFromNodes(manifestNodes map[string]interface{}) (*Manifest, error) {
	sources := make(map[string]map[string]interface{})
	models := make(map[string]map[string]interface{})
	seeds := make(map[string]map[string]interface{})
	snapshots := make(map[string]map[string]interface{})
	tests := make(map[string]map[string][]interface{})

	for _, v := range manifestNodes {
		node, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		resourceType, _ := node["resource_type"].(string)
		switch resourceType {
		case "source":
			id, _ := node["unique_id"].(string)
			sources[id] = normalizeTable(node)
		case "model":
			id, _ := node["unique_id"].(string)
			models[id] = normalizeTable(node)
		case "seed":
			id, _ := node["unique_id"].(string)
			seeds[id] = normalizeTable(node)
		case "snapshot":
			id, _ := node["unique_id"].(string)
			snapshots[id] = normalizeTable(node)
		case "test":
			// Traitement détaillé du noeud test
			if _, exists := node["test_metadata"]; !exists {
				continue
			}
			dependsRaw, ok := node["depends_on"].(map[string]interface{})
			if !ok {
				continue
			}
			nodesDep, ok := dependsRaw["nodes"].([]interface{})
			if !ok || len(nodesDep) == 0 {
				continue
			}
			testMeta, ok := node["test_metadata"].(map[string]interface{})
			if !ok {
				continue
			}
			testName, _ := testMeta["name"].(string)
			var tableID string
			if testName == "relationships" {
				if last, ok := nodesDep[len(nodesDep)-1].(string); ok {
					tableID = last
				}
			} else {
				if first, ok := nodesDep[0].(string); ok {
					tableID = first
				}
			}
			var columnName string
			if v, exists := node["column_name"]; exists {
				if s, ok := v.(string); ok {
					columnName = s
				}
			}
			if columnName == "" {
				if kwargs, ok := testMeta["kwargs"].(map[string]interface{}); ok {
					if v, exists := kwargs["column_name"]; exists {
						if s, ok := v.(string); ok {
							columnName = s
						}
					}
					if columnName == "" {
						if v, exists := kwargs["arg"]; exists {
							if s, ok := v.(string); ok {
								columnName = s
							}
						}
					}
				}
			}
			if columnName == "" {
				continue
			}
			columnName = strings.ToLower(columnName)
			if tests[tableID] == nil {
				tests[tableID] = make(map[string][]interface{})
			}
			tests[tableID][columnName] = append(tests[tableID][columnName], node)
		}
	}

	return &Manifest{
		Sources:   sources,
		Models:    models,
		Seeds:     seeds,
		Snapshots: snapshots,
		Tests:     tests,
	}, nil
}

func normalizeTable(table map[string]interface{}) map[string]interface{} {
	if cols, ok := table["columns"].(map[string]interface{}); ok {
		normCols := make(map[string]interface{})
		for _, v := range cols {
			if col, ok := v.(map[string]interface{}); ok {
				name := strings.ToLower(col["name"].(string))
				col["name"] = name
				normCols[name] = col
			}
		}
		table["columns"] = normCols
	}
	if pathStr, ok := table["original_file_path"].(string); ok {
		table["original_file_path"] = filepath.ToSlash(pathStr)
	}
	schema, _ := table["schema"].(string)
	name, _ := table["name"].(string)
	table["name"] = strings.ToLower(fmt.Sprintf("%s.%s", schema, name))
	return table
}

// --- Structures pour l'affichage détaillé en console ---
type TableCoverage struct {
	ModelName string
	Covered   int
	Total     int
}

type DetailedCoverageReport struct {
	TableReports []TableCoverage
	TotalCovered int
	TotalColumns int
	TableCount   int
	CovType      CoverageType
}

// --- Fonctions pour le calcul et l'affichage détaillé en console ---
func computeJSONReport(catalog Catalog, covType CoverageType) JSONReport {
	var tables []TableReport
	globalCovered := 0
	globalTotal := 0

	for _, table := range catalog.Tables {
		var cols []ColumnReport
		tableCovered := 0
		tableTotal := 0
		for _, col := range table.Columns {
			colTotal := 1
			colCovered := 0
			switch covType {
			case CoverageTypeDoc:
				if col.Doc {
					colCovered = 1
				}
			case CoverageTypeTest:
				if col.Test {
					colCovered = 1
				}
			}
			cols = append(cols, ColumnReport{
				Name:     col.Name,
				Covered:  colCovered,
				Total:    colTotal,
				Coverage: float64(colCovered) / float64(colTotal),
			})
			tableTotal += colTotal
			tableCovered += colCovered
		}
		tables = append(tables, TableReport{
			Name:     table.Name,
			Covered:  tableCovered,
			Total:    tableTotal,
			Coverage: float64(tableCovered) / float64(tableTotal),
			Columns:  cols,
		})
		globalTotal += tableTotal
		globalCovered += tableCovered
	}

	globalCoverage := 0.0
	if globalTotal > 0 {
		globalCoverage = float64(globalCovered) / float64(globalTotal)
	}
	return JSONReport{
		CovType:  string(covType),
		Covered:  globalCovered,
		Total:    globalTotal,
		Coverage: globalCoverage,
		Tables:   tables,
	}
}

func computeDetailedCoverage(catalog Catalog, covType CoverageType) DetailedCoverageReport {
	var reports []TableCoverage
	totalCovered := 0
	totalColumns := 0
	for _, table := range catalog.Tables {
		tCovered := 0
		tTotal := 0
		for _, col := range table.Columns {
			tTotal++
			switch covType {
			case CoverageTypeDoc:
				if col.Doc {
					tCovered++
				}
			case CoverageTypeTest:
				if col.Test {
					tCovered++
				}
			}
		}
		reports = append(reports, TableCoverage{
			ModelName: table.Name,
			Covered:   tCovered,
			Total:     tTotal,
		})
		totalCovered += tCovered
		totalColumns += tTotal
	}
	return DetailedCoverageReport{
		TableReports: reports,
		TotalCovered: totalCovered,
		TotalColumns: totalColumns,
		TableCount:   len(catalog.Tables),
		CovType:      covType,
	}
}

func printDetailedCoverageReport(report DetailedCoverageReport) {

	fmt.Printf("%s ✅ Analyse terminée : %d tables, %d colonnes analysées.\n\n",
		currentLogPrefix(), report.TableCount, report.TotalColumns)
	fmt.Printf("📊 Coverage Report (%s)\n", strings.ToUpper(string(report.CovType)))
	fmt.Println()

	// Création d'un nouvel objet tablewriter
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Model", "Columns Ratio", "Coverage"})
	table.SetBorder(false)
	table.SetCenterSeparator("│")
	table.SetColumnAlignment([]int{
		tablewriter.ALIGN_LEFT, tablewriter.ALIGN_CENTER, tablewriter.ALIGN_RIGHT,
	})

	for _, tr := range report.TableReports {
		ratio := fmt.Sprintf("(%d/%d)", tr.Covered, tr.Total)
		coverage := "0.0%"
		if tr.Total > 0 {
			coverage = fmt.Sprintf("%.1f%%", float64(tr.Covered)/float64(tr.Total)*100)
		}
		table.Append([]string{tr.ModelName, ratio, coverage})
	}

	totalRatio := fmt.Sprintf("(%d/%d)", report.TotalCovered, report.TotalColumns)
	totalCoverage := "0.0%"
	if report.TotalColumns > 0 {
		totalCoverage = fmt.Sprintf("%.1f%%", float64(report.TotalCovered)/float64(report.TotalColumns)*100)
	}
	table.SetFooter([]string{"TOTAL", totalRatio, totalCoverage})

	table.Render()
}

func currentLogPrefix() string {
	return time.Now().Format("02-01-2006 15:04:05")
}

func checkManifestVersion(manifestJSON map[string]interface{}) {
	metadata, ok := manifestJSON["metadata"].(map[string]interface{})
	if !ok {
		return
	}
	version, _ := metadata["dbt_schema_version"].(string)
	found := false
	for _, v := range SupportedManifestSchemaVersions {
		if version == v {
			found = true
			break
		}
	}
	if !found {
		log.Printf("warning: manifest version %s non supportée. Versions supportées: %v", version, SupportedManifestSchemaVersions)
	}
}

func loadManifest(projectDir string, runArtifactsDir string) (*Manifest, error) {
	var manifestPath string
	if runArtifactsDir == "" {
		manifestPath = filepath.Join(projectDir, "target", "manifest.json")
	} else {
		manifestPath = filepath.Join(runArtifactsDir, "manifest.json")
	}
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("manifest.json non trouvé dans %s", manifestPath)
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var manifestJSON map[string]interface{}
	if err := json.Unmarshal(data, &manifestJSON); err != nil {
		return nil, err
	}
	checkManifestVersion(manifestJSON)
	nodes := make(map[string]interface{})
	if sources, ok := manifestJSON["sources"].(map[string]interface{}); ok {
		for k, v := range sources {
			nodes[k] = v
		}
	}
	if n, ok := manifestJSON["nodes"].(map[string]interface{}); ok {
		for k, v := range n {
			nodes[k] = v
		}
	}
	return ManifestFromNodes(nodes)
}

func loadCatalog(projectDir string, runArtifactsDir string, manifest *Manifest) (Catalog, error) {
	var catalogPath string
	if runArtifactsDir == "" {
		catalogPath = filepath.Join(projectDir, "target", "catalog.json")
	} else {
		catalogPath = filepath.Join(runArtifactsDir, "catalog.json")
	}
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return Catalog{}, fmt.Errorf("catalog.json non trouvé dans %s", catalogPath)
	}
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return Catalog{}, err
	}
	var catalogJSON map[string]interface{}
	if err := json.Unmarshal(data, &catalogJSON); err != nil {
		return Catalog{}, err
	}
	var catalogNodes []interface{}
	for _, key := range []string{"sources", "nodes"} {
		if group, ok := catalogJSON[key].(map[string]interface{}); ok {
			for id, node := range group {
				if strings.HasPrefix(id, "test.") {
					continue
				}
				catalogNodes = append(catalogNodes, node)
			}
		}
	}
	return CatalogFromNodes(catalogNodes, manifest)
}

func loadFiles(projectDir string, runArtifactsDir string) (Catalog, error) {
	if runArtifactsDir == "" {
		log.Printf("Chargement des fichiers depuis le projet : %s", projectDir)
	} else {
		log.Printf("Chargement des fichiers depuis le dossier personnalisé : %s", runArtifactsDir)
	}
	manifest, err := loadManifest(projectDir, runArtifactsDir)
	if err != nil {
		return Catalog{}, err
	}
	catalog, err := loadCatalog(projectDir, runArtifactsDir, manifest)
	if err != nil {
		return Catalog{}, err
	}
	// Mise à jour des colonnes avec les infos de doc et test depuis le manifest.
	for tableID, table := range catalog.Tables {
		var manifestTable map[string]interface{}
		if v, ok := manifest.Sources[tableID]; ok {
			manifestTable = v
		} else if v, ok := manifest.Models[tableID]; ok {
			manifestTable = v
		} else if v, ok := manifest.Seeds[tableID]; ok {
			manifestTable = v
		} else if v, ok := manifest.Snapshots[tableID]; ok {
			manifestTable = v
		}
		var manifestColumns map[string]interface{}
		if manifestTable != nil {
			if mc, ok := manifestTable["columns"].(map[string]interface{}); ok {
				manifestColumns = mc
			}
		}
		manifestTableTests := manifest.Tests[tableID]
		for colName, col := range table.Columns {
			var colInfo map[string]interface{}
			if manifestColumns != nil {
				if v, ok := manifestColumns[colName]; ok {
					if ci, ok := v.(map[string]interface{}); ok {
						colInfo = ci
					}
				}
			}
			var desc interface{}
			if colInfo != nil {
				desc = colInfo["description"]
			}
			col.Doc = IsValidDoc(desc)
			var testsForCol []interface{}
			if manifestTableTests != nil {
				testsForCol = manifestTableTests[colName]
			}
			col.Test = IsValidTest(testsForCol)
			table.Columns[colName] = col
		}
		catalog.Tables[tableID] = table
	}
	return catalog, nil
}

func writeCoverageReport(report JSONReport, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	log.Printf("Écriture du rapport dans %s", path)
	return os.WriteFile(path, data, 0644)
}

func doCompute(projectDir, runArtifactsDir, output string, covType CoverageType, modelPathFilter []string) error {
	catalog, err := loadFiles(projectDir, runArtifactsDir)
	if err != nil {
		return err
	}
	if len(modelPathFilter) > 0 {
		catalog = catalog.FilterTables(modelPathFilter)
		if len(catalog.Tables) == 0 {
			return errors.New("aucune table après filtrage, vérifiez path_filter")
		}
	}

	detailedReport := computeDetailedCoverage(catalog, covType)
	printDetailedCoverageReport(detailedReport)

	jsonReport := computeJSONReport(catalog, covType)
	if err := writeCoverageReport(jsonReport, output); err != nil {
		return err
	}
	return nil
}

func main() {
	var (
		projectDir      = flag.String("dbt_dir", ".", "Chemin du projet dbt")
		runArtifactsDir = flag.String("target_dir", "target", "Chemin personnalisé pour les fichiers catalog et manifest")
		output          = flag.String("output", "coverage.json", "Fichier de sortie du rapport de couverture (JSON)")
		covTypeStr      = flag.String("type", "test", "Type de couverture à calculer (doc ou test)")
		modelFilter     = flag.String("path_filter", "", "Filtre de chemin pour les modèles (séparé par des virgules)")
		verbose         = flag.Bool("verbose", false, "Activer les logs détaillés")
	)
	flag.Parse()

	if *verbose {
		log.SetFlags(log.LstdFlags)
	} else {
		log.SetOutput(io.Discard)
	}

	covType := CoverageType(*covTypeStr)
	var filters []string
	if *modelFilter != "" {
		filters = strings.Split(*modelFilter, ",")
	}

	if err := doCompute(*projectDir, *runArtifactsDir, *output, covType, filters); err != nil {
		log.Fatalf("Erreur lors du calcul de la couverture: %v", err)
	}
}

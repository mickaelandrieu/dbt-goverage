// main_test.go
package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDbtCoverageGoOutput(t *testing.T) {
	// Définir un fichier temporaire pour la sortie
	outputFile := filepath.Join(os.TempDir(), "test-output.json")
	defer os.Remove(outputFile) // Nettoyage après le test

	// Créer un contexte avec timeout (par exemple 30 secondes)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Construire la commande pour exécuter le binaire via "go run"
	cmd := exec.CommandContext(ctx, "go", "run", "main.go",
		"--type", "doc",
		"--path_filter", "models/marts/rbbu",
		"--output", outputFile,
		"--target_dir", "tests/target",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Erreur lors de l'exécution du binaire : %v\nSortie : %s", err, string(output))
	}

	// Vérifier que le fichier de sortie existe.
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatalf("Le fichier de sortie %s n'a pas été créé", outputFile)
	}

	// Lire le contenu du fichier JSON avec os.ReadFile
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Erreur lors de la lecture du fichier JSON : %v", err)
	}

	// Décoder le JSON en utilisant la structure exportée JSONReport du main.go
	var report JSONReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("Erreur lors du décodage du JSON : %v", err)
	}

	// Vérifier que certaines informations sont correctes.
	if report.Total <= 0 {
		t.Errorf("Le nombre total de colonnes doit être > 0, obtenu : %d", report.Total)
	}
	if report.Coverage < 0.0 || report.Coverage > 1.0 {
		t.Errorf("La couverture globale (%f) doit être comprise entre 0 et 1", report.Coverage)
	}
	if len(report.Tables) == 0 {
		t.Errorf("Aucune table trouvée dans le rapport")
	}

	// Par exemple, vérifier qu'une table spécifique existe
	expectedTable := "mart_rbbu.rbbu__demain_mentores"
	found := false
	for _, table := range report.Tables {
		if table.Name == expectedTable {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("La table %s n'a pas été trouvée dans le rapport", expectedTable)
	}
}

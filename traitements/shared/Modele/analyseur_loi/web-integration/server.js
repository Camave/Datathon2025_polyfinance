const express = require('express');
const cors = require('cors');
const { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const s3Client = new S3Client({ 
    region: process.env.AWS_REGION || 'us-east-1'
});

const ORCHESTRATOR_PATH = path.join(__dirname, '..', 'orchestrator.py');
const CONFIG_PATH = path.join(__dirname, '..', 'config.json');
const S3_BUCKET = 'csv-file-store-740fdb60';
const S3_BASE_PATH = 'dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/';

// Route pour exécuter un agent spécifique
app.post('/api/agent/invoke', async (req, res) => {
    try {
        const { agentId, userInput, selectedLaws } = req.body;
        
        // Sauvegarder l'input dans S3
        const inputKey = `${S3_BASE_PATH}inputs/${Date.now()}_agent${agentId}.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: inputKey,
            Body: JSON.stringify({ 
                input: userInput, 
                agentId: agentId,
                selectedLaws: selectedLaws || [],
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        // Exécuter l'agent via l'orchestrateur Python
        const result = await executeAgent(agentId, selectedLaws);
        
        // Sauvegarder l'output dans S3
        const outputKey = `${S3_BASE_PATH}outputs/${Date.now()}_agent${agentId}.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: outputKey,
            Body: JSON.stringify({ 
                output: result,
                inputKey: inputKey,
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        res.json({
            success: true,
            data: result,
            s3Keys: { input: inputKey, output: outputKey }
        });

    } catch (error) {
        console.error('Erreur:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Route pour analyser les lois sélectionnées
app.post('/api/analyze', async (req, res) => {
    try {
        const { userInput, selectedLaws } = req.body;
        
        if (!selectedLaws || selectedLaws.length === 0) {
            return res.status(400).json({ 
                success: false, 
                error: 'Aucune loi sélectionnée' 
            });
        }
        
        // Sauvegarder l'input dans S3
        const inputKey = `${S3_BASE_PATH}inputs/${Date.now()}_law_analysis.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: inputKey,
            Body: JSON.stringify({ 
                input: userInput,
                selectedLaws: selectedLaws,
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        // Exécuter l'analyse complète
        const result = await executeFullPipeline(selectedLaws);
        
        // Sauvegarder l'output dans S3
        const outputKey = `${S3_BASE_PATH}outputs/${Date.now()}_law_analysis.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: outputKey,
            Body: JSON.stringify({ 
                output: result,
                inputKey: inputKey,
                selectedLaws: selectedLaws,
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        res.json({
            success: true,
            data: result,
            s3Keys: { input: inputKey, output: outputKey }
        });

    } catch (error) {
        console.error('Erreur:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Route pour exécuter le pipeline complet
app.post('/api/pipeline/execute', async (req, res) => {
    try {
        const { userInput, selectedLaws } = req.body;
        
        // Sauvegarder l'input dans S3
        const inputKey = `${S3_BASE_PATH}inputs/${Date.now()}_full_pipeline.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: inputKey,
            Body: JSON.stringify({ 
                input: userInput,
                type: 'full_pipeline',
                selectedLaws: selectedLaws || [],
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        // Exécuter le pipeline complet
        const result = await executeFullPipeline(selectedLaws);
        
        // Sauvegarder l'output dans S3
        const outputKey = `${S3_BASE_PATH}outputs/${Date.now()}_full_pipeline.json`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: outputKey,
            Body: JSON.stringify({ 
                output: result,
                inputKey: inputKey,
                timestamp: new Date().toISOString() 
            }),
            ContentType: 'application/json'
        }));

        res.json({
            success: true,
            data: result,
            s3Keys: { input: inputKey, output: outputKey }
        });

    } catch (error) {
        console.error('Erreur:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Route pour récupérer l'historique depuis S3
app.get('/api/history', async (req, res) => {
    try {
        const command = new ListObjectsV2Command({
            Bucket: S3_BUCKET,
            Prefix: `${S3_BASE_PATH}outputs/`,
            MaxKeys: 20
        });
        
        const response = await s3Client.send(command);
        
        res.json({
            success: true,
            files: response.Contents || []
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Route pour obtenir le statut d'exécution
app.get('/api/status', async (req, res) => {
    try {
        const result = await getExecutionStatus();
        res.json({
            success: true,
            data: result
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Route pour créer une modélisation
app.post('/api/model', async (req, res) => {
    try {
        const { csvPath } = req.body;
        
        // Exécuter la modélisation
        const result = await createPortfolioModel(csvPath);
        
        res.json({
            success: true,
            data: result
        });

    } catch (error) {
        console.error('Erreur modélisation:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Route pour lister les lois disponibles
app.get('/api/laws', async (req, res) => {
    try {
        const command = new ListObjectsV2Command({
            Bucket: S3_BUCKET,
            Prefix: `${S3_BASE_PATH}lois/`,
            MaxKeys: 100
        });
        
        const response = await s3Client.send(command);
        const laws = (response.Contents || [])
            .filter(obj => obj.Key.endsWith('.txt') || obj.Key.endsWith('.pdf') || obj.Key.endsWith('.json'))
            .map(obj => ({
                key: obj.Key,
                name: obj.Key.split('/').pop(),
                lastModified: obj.LastModified
            }));
        
        res.json({
            success: true,
            laws: laws
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Fonction pour exécuter un agent spécifique
function executeAgent(agentId, selectedLaws = []) {
    return new Promise((resolve, reject) => {
        const args = ['single', agentId.toString()];
        if (selectedLaws.length > 0) {
            args.push('--laws', selectedLaws.join(','));
        }
        const python = spawn('python3', [ORCHESTRATOR_PATH, ...args], {
            cwd: path.join(__dirname, '..')
        });
        
        let stdout = '';
        let stderr = '';
        
        python.stdout.on('data', (data) => {
            stdout += data.toString();
        });
        
        python.stderr.on('data', (data) => {
            stderr += data.toString();
        });
        
        python.on('close', (code) => {
            if (code === 0) {
                resolve({
                    agentId: agentId,
                    status: 'completed',
                    output: stdout,
                    timestamp: new Date().toISOString()
                });
            } else {
                reject(new Error(`Agent ${agentId} failed: ${stderr}`));
            }
        });
    });
}

// Fonction pour exécuter le pipeline complet
function executeFullPipeline(selectedLaws = []) {
    return new Promise((resolve, reject) => {
        const args = [];
        if (selectedLaws.length > 0) {
            args.push('--laws', selectedLaws.join(','));
        }
        const python = spawn('python3', [ORCHESTRATOR_PATH, ...args], {
            cwd: path.join(__dirname, '..')
        });
        
        let stdout = '';
        let stderr = '';
        
        python.stdout.on('data', (data) => {
            stdout += data.toString();
        });
        
        python.stderr.on('data', (data) => {
            stderr += data.toString();
        });
        
        python.on('close', (code) => {
            if (code === 0) {
                resolve({
                    type: 'full_pipeline',
                    status: 'completed',
                    output: stdout,
                    timestamp: new Date().toISOString()
                });
            } else {
                reject(new Error(`Pipeline failed: ${stderr}`));
            }
        });
    });
}

// Fonction pour obtenir le statut d'exécution
function getExecutionStatus() {
    return new Promise((resolve, reject) => {
        const python = spawn('python3', [ORCHESTRATOR_PATH, 'status'], {
            cwd: path.join(__dirname, '..')
        });
        
        let stdout = '';
        let stderr = '';
        
        python.stdout.on('data', (data) => {
            stdout += data.toString();
        });
        
        python.stderr.on('data', (data) => {
            stderr += data.toString();
        });
        
        python.on('close', (code) => {
            if (code === 0) {
                try {
                    const status = JSON.parse(stdout);
                    resolve(status);
                } catch (e) {
                    resolve({ raw_output: stdout });
                }
            } else {
                reject(new Error(`Status check failed: ${stderr}`));
            }
        });
    });
}

// Fonction pour créer une modélisation du portfolio
function createPortfolioModel(csvPath) {
    return new Promise(async (resolve, reject) => {
        try {
            // Télécharger le CSV depuis S3
            const csvData = await s3Client.send(new GetObjectCommand({
                Bucket: S3_BUCKET,
                Key: csvPath
            }));
            
            const csvContent = await csvData.Body.transformToString();
            const lines = csvContent.split('\n').filter(line => line.trim());
            
            // Analyse simple du CSV
            const headers = lines[0].split(',');
            const data = lines.slice(1).map(line => {
                const values = line.split(',');
                const row = {};
                headers.forEach((header, index) => {
                    row[header.trim()] = values[index]?.trim();
                });
                return row;
            });
            
            // Création de métriques simples
            const totalCompanies = data.length;
            const impactedCompanies = data.filter(row => 
                parseFloat(row.impact_coefficient || 0) > 0
            ).length;
            
            const avgImpact = data.reduce((sum, row) => 
                sum + parseFloat(row.impact_coefficient || 0), 0
            ) / totalCompanies;
            
            resolve({
                modelType: 'Legislative Impact Portfolio Model',
                accuracy: `${((impactedCompanies / totalCompanies) * 100).toFixed(1)}%`,
                metrics: {
                    totalCompanies,
                    impactedCompanies,
                    averageImpact: avgImpact.toFixed(3),
                    timestamp: new Date().toISOString()
                },
                predictions: data.slice(0, 5).map(row => ({
                    company: row.company || row.symbol,
                    sector: row.sector,
                    impact: parseFloat(row.impact_coefficient || 0).toFixed(3)
                }))
            });
            
        } catch (error) {
            reject(error);
        }
    });
}

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Serveur démarré sur le port ${PORT}`);
});
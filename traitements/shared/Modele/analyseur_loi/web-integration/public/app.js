const API_URL = 'http://localhost:3000/api';

async function analyzeLaws() {
    const userInput = document.getElementById('userInput').value;
    const loader = document.getElementById('loader');
    const responseDiv = document.getElementById('response');

    const selectedLaws = Array.from(document.getElementById('lawSelect').selectedOptions)
        .map(option => option.value)
        .filter(value => value !== 'loading');
    
    if (selectedLaws.length === 0) {
        responseDiv.innerHTML = '<p class="error">❌ Veuillez sélectionner au moins une loi</p>';
        return;
    }

    loader.style.display = 'block';
    responseDiv.innerHTML = '';
    
    // Démarrer l'animation des étapes
    simulateAnalysisSteps();

    try {
        const response = await fetch(`${API_URL}/analyze`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                userInput: userInput || 'Analyse complète des lois sélectionnées',
                selectedLaws: selectedLaws
            })
        });

        const data = await response.json();

        if (data.success) {
            displayResponse(data.data, data.s3Keys);
        } else {
            responseDiv.innerHTML = `<p class="error">❌ Erreur: ${data.error}</p>`;
        }
    } catch (error) {
        responseDiv.innerHTML = `<p class="error">❌ Erreur de connexion: ${error.message}</p>`;
    } finally {
        loader.style.display = 'none';
    }
}

async function getStatus() {
    const loader = document.getElementById('loader');
    const responseDiv = document.getElementById('response');

    loader.style.display = 'block';

    try {
        const response = await fetch(`${API_URL}/status`);
        const data = await response.json();

        if (data.success) {
            displayStatus(data.data);
        } else {
            responseDiv.innerHTML = `<p class="error">❌ Erreur: ${data.error}</p>`;
        }
    } catch (error) {
        responseDiv.innerHTML = `<p class="error">❌ Erreur: ${error.message}</p>`;
    } finally {
        loader.style.display = 'none';
    }
}

function displayResponse(data, s3Keys) {
    const responseDiv = document.getElementById('response');
    const modelBtn = document.getElementById('modelBtn');
    
    responseDiv.innerHTML = `
        <div class="result">
            <h3>✅ Résultat de l'exécution:</h3>
            <div class="result-content">
                <p><strong>Type:</strong> ${data.type || 'Agent'}</p>
                <p><strong>Statut:</strong> ${data.status}</p>
                <p><strong>Timestamp:</strong> ${data.timestamp}</p>
                <div class="output-section">
                    <h4>Output:</h4>
                    <pre>${data.output}</pre>
                </div>
            </div>
            <div class="s3-info">
                <h4>📁 Fichiers S3:</h4>
                <small>Input: ${s3Keys.input}</small><br>
                <small>Output: ${s3Keys.output}</small><br>
                <small>CSV Résultat: dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/2025-08-15_composition_sp500_updated.csv</small>
            </div>
        </div>
    `;
    
    // Activer le bouton de modélisation
    modelBtn.disabled = false;
}

function displayStatus(data) {
    const responseDiv = document.getElementById('response');
    
    responseDiv.innerHTML = `
        <div class="result">
            <h3>📊 Statut d'exécution:</h3>
            <div class="status-content">
                <p><strong>Étapes totales:</strong> ${data.total_steps || 0}</p>
                ${data.last_execution ? `
                    <div class="last-execution">
                        <h4>Dernière exécution:</h4>
                        <p>Agent: ${data.last_execution.agent_id}</p>
                        <p>Statut: ${data.last_execution.status}</p>
                        <p>Timestamp: ${data.last_execution.timestamp}</p>
                    </div>
                ` : '<p>Aucune exécution récente</p>'}
                <div class="execution-log">
                    <h4>Log d'exécution:</h4>
                    <pre>${JSON.stringify(data.execution_log || [], null, 2)}</pre>
                </div>
            </div>
        </div>
    `;
}

async function loadHistory() {
    const historyDiv = document.getElementById('historyList');
    
    try {
        const response = await fetch(`${API_URL}/history`);
        const data = await response.json();

        if (data.success) {
            historyDiv.innerHTML = data.files
                .map(file => `
                    <div class="history-item">
                        📄 ${file.Key.split('/').pop()} 
                        <small>(${new Date(file.LastModified).toLocaleString()})</small>
                    </div>
                `)
                .join('');
        }
    } catch (error) {
        historyDiv.innerHTML = `<p class="error">❌ Erreur: ${error.message}</p>`;
    }
}

async function loadLaws() {
    const lawSelect = document.getElementById('lawSelect');
    
    try {
        const response = await fetch(`${API_URL}/laws`);
        
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.laws.length > 0) {
                lawSelect.innerHTML = data.laws
                    .map(law => `<option value="${law.key}">${law.name}</option>`)
                    .join('');
                return;
            }
        }
    } catch (error) {
        console.log('Utilisation des lois par défaut');
    }
    
    // Lois réelles du bucket S3
    lawSelect.innerHTML = `
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/1.DIRECTIVE (UE) 20192161 DU PARLEMENT EUROPÉEN ET DU CONSEIL.html">Directive UE 2019/2161</option>
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/2.H.R.1 - One Big Beautiful Bill Act.xml">H.R.1 - One Big Beautiful Bill Act</option>
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/3.H.R.5376 - Inflation Reduction Act of 2022.xml">Inflation Reduction Act 2022</option>
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/4.REGULATION (EU) 20241689 OF THE EUROPEAN PARLIAMENT AND OF THE COUNCIL.html">Règlement UE 2024/1689</option>
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/5.中华人民共和国能源法__中国政府网.html">Loi Énergie Chine</option>
        <option value="dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/lois/6.人工知能関連技術の研究開発及び活用の推進に関する法律.html">Loi IA Japon</option>
    `;
}

async function createModel() {
    const loader = document.getElementById('loader');
    const responseDiv = document.getElementById('response');

    loader.style.display = 'block';
    
    // Animation pour la modélisation
    simulateModelingSteps();

    try {
        const response = await fetch(`${API_URL}/model`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                csvPath: 'dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/2025-08-15_composition_sp500_updated.csv'
            })
        });

        const data = await response.json();

        if (data.success) {
            displayModelResults(data.data);
        } else {
            responseDiv.innerHTML += `<p class="error">❌ Erreur modélisation: ${data.error}</p>`;
        }
    } catch (error) {
        responseDiv.innerHTML += `<p class="error">❌ Erreur: ${error.message}</p>`;
    } finally {
        loader.style.display = 'none';
    }
}

function displayModelResults(data) {
    const responseDiv = document.getElementById('response');
    
    responseDiv.innerHTML += `
        <div class="model-result">
            <h3>📊 Modélisation Créée:</h3>
            <div class="model-content">
                <p><strong>Modèle:</strong> ${data.modelType || 'Portfolio Impact Model'}</p>
                <p><strong>Précision:</strong> ${data.accuracy || 'N/A'}</p>
                <div class="metrics">
                    <h4>Métriques:</h4>
                    <pre>${JSON.stringify(data.metrics || {}, null, 2)}</pre>
                </div>
                <div class="predictions">
                    <h4>Prédictions:</h4>
                    <pre>${JSON.stringify(data.predictions || [], null, 2)}</pre>
                </div>
            </div>
        </div>
    `;
}

function simulateAnalysisSteps() {
    const steps = [
        { text: '📄 Agent 1 - Extraction du portfolio...', progress: 16 },
        { text: '🏢 Agent 2 - Classification des secteurs...', progress: 32 },
        { text: '📜 Agent 3 - Analyse d\'impact législatif...', progress: 48 },
        { text: '💰 Agent 4 - Modélisation des flux...', progress: 64 },
        { text: '📈 Agent 5 - Quantification des impacts...', progress: 80 },
        { text: '🔄 Agent 6 - Réallocation du portfolio...', progress: 100 }
    ];
    
    animateSteps(steps, 'Analyse terminée');
}

function simulateModelingSteps() {
    const steps = [
        { text: '📁 Chargement des données CSV...', progress: 25 },
        { text: '🧠 Entraînement du modèle...', progress: 50 },
        { text: '📈 Calcul des métriques...', progress: 75 },
        { text: '🎯 Génération des prédictions...', progress: 100 }
    ];
    
    animateSteps(steps, 'Modélisation terminée');
}

function animateSteps(steps, finalMessage) {
    let currentStep = 0;
    
    const interval = setInterval(() => {
        if (currentStep < steps.length) {
            const step = steps[currentStep];
            document.getElementById('loadingText').textContent = step.text;
            document.getElementById('progressFill').style.width = step.progress + '%';
            document.getElementById('agentStatus').textContent = `Étape ${currentStep + 1}/${steps.length}`;
            currentStep++;
        } else {
            document.getElementById('loadingText').textContent = '✅ Finalisation...';
            document.getElementById('agentStatus').textContent = finalMessage;
            clearInterval(interval);
        }
    }, 1500);
}

// Charger les lois au démarrage
document.addEventListener('DOMContentLoaded', loadLaws);
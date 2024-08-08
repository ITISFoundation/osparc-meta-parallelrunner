const express = require('express');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const port = 8888;

app.use(express.json());

const dataFile = path.join(__dirname, 'jobs.json');

app.get('/api/jobs', async (req, res) => {
  try {
    const data = await fs.readFile(dataFile, 'utf8');
    res.json(JSON.parse(data));
  } catch (err) {
    console.error('Error reading file:', err);
    res.status(500).json({ error: 'Error reading job data' });
  }
});

app.put('/api/jobs/:id', async (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  try {
    const data = await fs.readFile(dataFile, 'utf8');
    const jobs = JSON.parse(data);

    if (!jobs[id]) {
      return res.status(404).json({ error: 'Job not found' });
    }

    jobs[id].status = status;
    await fs.writeFile(dataFile, JSON.stringify(jobs, null, 2));
    res.json(jobs[id]);
  } catch (err) {
    console.error('Error updating job:', err);
    res.status(500).json({ error: 'Error updating job' });
  }
});

app.use(express.static(path.join(__dirname, 'dist')));

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});


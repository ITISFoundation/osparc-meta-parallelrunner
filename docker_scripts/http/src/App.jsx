import React, { useState, useEffect } from 'react'

const statusColors = {
  todo: 'bg-gray-700',
  running: 'bg-blue-700',
  done: 'bg-green-700',
  failed: 'bg-red-700'
};

const statusOptions = ['todo', 'running', 'done', 'failed'];

const JobCard = ({ job, onStatusChange }) => (
  <div
    className={`p-4 mb-2 rounded shadow ${statusColors[job.status]} transition-all duration-300 ease-in-out`}
  >
    <h3 className="font-bold text-white">{job.name}</h3>
    <p className="mb-2 text-gray-300">{job.description}</p>
    <select 
      value={job.status} 
      onChange={(e) => onStatusChange(job.id, e.target.value)}
      className="mt-2 p-1 rounded border border-gray-600 bg-gray-800 text-white"
    >
      {statusOptions.map(status => (
        <option key={status} value={status}>
          {status.charAt(0).toUpperCase() + status.slice(1)}
        </option>
      ))}
    </select>
  </div>
);

const StatusColumn = ({ title, jobs, onStatusChange }) => (
  <div className="flex-1 flex flex-col h-full p-4 overflow-hidden">
    <h2 className="text-xl font-bold mb-4 text-gray-200">{title}</h2>
    <div className="flex-1 overflow-y-auto pr-2">
      {jobs.map(job => (
        <JobCard key={job.id} job={job} onStatusChange={onStatusChange} />
      ))}
    </div>
  </div>
);

const Dashboard = () => {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchJobs = async () => {
      try {
        const response = await fetch('http://localhost:3001/api/jobs');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        setJobs(data);
        setLoading(false);
      } catch (e) {
        console.error("Failed to fetch jobs:", e);
        setError("Failed to load jobs. Please try again later.");
        setLoading(false);
      }
    };

    fetchJobs();

    const intervalId = setInterval(fetchJobs, 30000);

    return () => clearInterval(intervalId);
  }, []);

  const handleStatusChange = async (id, newStatus) => {
    try {
      const response = await fetch(`http://localhost:3001/api/jobs/${id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ status: newStatus }),
      });
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      setJobs(jobs.map(job => 
        job.id === id ? { ...job, status: newStatus } : job
      ));
    } catch (e) {
      console.error("Failed to update job status:", e);
      // Handle the error appropriately in your UI
    }
  };

  const jobsByStatus = {
    todo: jobs.filter(job => job.status === 'todo'),
    running: jobs.filter(job => job.status === 'running'),
    done: jobs.filter(job => job.status === 'done'),
    failed: jobs.filter(job => job.status === 'failed')
  };

  if (loading) {
    return <div className="flex h-screen bg-gray-900 text-white items-center justify-center">Loading...</div>;
  }

  if (error) {
    return <div className="flex h-screen bg-gray-900 text-white items-center justify-center">{error}</div>;
  }

  return (
    <div className="flex h-screen bg-gray-900 text-white">
      <StatusColumn title="To Do" jobs={jobsByStatus.todo} onStatusChange={handleStatusChange} />
      <StatusColumn title="Running" jobs={jobsByStatus.running} onStatusChange={handleStatusChange} />
      <StatusColumn title="Done" jobs={jobsByStatus.done} onStatusChange={handleStatusChange} />
      <StatusColumn title="Failed" jobs={jobsByStatus.failed} onStatusChange={handleStatusChange} />
    </div>
  );
};

function App() {
  return (
    <div className="App">
      <Dashboard />
    </div>
  );
}

export default App;

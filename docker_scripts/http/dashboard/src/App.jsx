import React, { useState, useEffect, useCallback } from 'react'

const statusColors = {
  todo: 'bg-gray-300',
  running: 'bg-blue-700',
  done: 'bg-green-700',
  failed: 'bg-red-700'
};

const StatusIcon = ({ status }) => {
  switch (status) {
    case 'todo':
      return (
        <svg className="w-6 h-6 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l4-4 4 4m0 6l-4 4-4-4" />
        </svg>
      );
    case 'running':
      return (
        <svg className="w-6 h-6 text-blue-300 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
        </svg>
      );
    case 'done':
      return (
        <svg className="w-6 h-6 text-green-300" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
        </svg>
      );
    case 'failed':
      return (
        <svg className="w-6 h-6 text-red-300" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      );
    default:
      return null;
  }
};

const formatDuration = (seconds) => {
  if (seconds === null) return 'Unknown';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
};

const ProgressBar = ({ jobsByStatus, totalETA }) => {
  const total = Object.values(jobsByStatus).reduce((acc, jobs) => acc + Object.keys(jobs).length, 0);
  const widths = {
    done: (Object.keys(jobsByStatus.done).length / total) * 100,
    running: (Object.keys(jobsByStatus.running).length / total) * 100,
    todo: (Object.keys(jobsByStatus.todo).length / total) * 100,
    failed: (Object.keys(jobsByStatus.failed).length / total) * 100
  };

  return (
    <div className="w-full mb-4">
      <div className="h-8 flex mb-2">
        {['done', 'running', 'todo', 'failed'].map((status) => (
          <div
            key={status}
            className={`${statusColors[status]} relative overflow-hidden`}
            style={{ width: `${widths[status]}%` }}
          >
            {status === 'running' && (
              <div className="absolute inset-0 opacity-50">
                <div className="animate-progress-mac w-full h-full bg-white" />
              </div>
            )}
          </div>
        ))}
      </div>
      <div className="text-sm text-gray-300">
        <span>Estimated Time Remaining: {formatDuration(totalETA)}</span>
      </div>
    </div>
  );
};

const JobCard = ({ job }) => (
  <div className="relative mb-2 group">
    <div className={`p-3 rounded shadow ${statusColors[job.status]} transition-all duration-300 ease-in-out`}>
      <div className="flex justify-between items-center">
        <h3 className={`font-bold ${job.status === 'todo' ? 'text-gray-800' : 'text-white'} truncate`}>{job.name}</h3>
        <StatusIcon status={job.status} />
      </div>
      {job.status === 'running' && (
        <div className="mt-2 text-sm text-white">
          <p>Running Time: {formatDuration(job.runningTime)}</p>
          <p>ETA: {formatDuration(job.eta)}</p>
        </div>
      )}
      {job.status === 'done' && (
        <div className="mt-2 text-sm text-white">
          <p>Completed in: {formatDuration(job.completionTime)}</p>
        </div>
      )}
    </div>
    <div className={`absolute top-0 left-0 w-full p-3 rounded shadow ${statusColors[job.status]} opacity-0 group-hover:opacity-100 transition-all duration-300 ease-in-out z-10`}>
      <div className="flex justify-between items-center mb-2">
        <h3 className={`font-bold ${job.status === 'todo' ? 'text-gray-800' : 'text-white'}`}>{job.name}</h3>
        <StatusIcon status={job.status} />
      </div>
      <p className={job.status === 'todo' ? 'text-gray-700' : 'text-gray-300'}>{job.description}</p>
      {job.status === 'running' && (
        <div className="mt-2 text-sm text-white">
          <p>Running Time: {formatDuration(job.runningTime)}</p>
          <p>ETA: {formatDuration(job.eta)}</p>
        </div>
      )}
      {job.status === 'done' && (
        <div className="mt-2 text-sm text-white">
          <p>Completed in: {formatDuration(job.completionTime)}</p>
        </div>
      )}
    </div>
  </div>
);

const StatusColumn = ({ title, jobs }) => (
  <div className="flex-1 flex flex-col h-full p-4 overflow-hidden border-r border-gray-700">
    <h2 className="text-xl font-bold mb-2 text-gray-200">{title}</h2>
    <p className="text-sm text-gray-400 mb-4">Jobs: {Object.keys(jobs).length}</p>
    <div className="flex-1 overflow-y-auto pr-2">
      {Object.entries(jobs).map(([id, job]) => (
        <JobCard key={id} job={job} />
      ))}
    </div>
  </div>
);

const Dashboard = () => {
  const [jobs, setJobs] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [totalETA, setTotalETA] = useState(null);

  const fetchJobs = useCallback(async () => {
    try {
      const response = await fetch('/api/jobs');
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setJobs(data.jobs);
      setTotalETA(data.totalETA);
      setLoading(false);
    } catch (e) {
      console.error("Failed to fetch jobs:", e);
      setError("Failed to load jobs. Please try again later.");
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
    const intervalId = setInterval(fetchJobs, 1000); // Refresh every 1 second

    return () => clearInterval(intervalId);
  }, [fetchJobs]);

  const jobsByStatus = Object.entries(jobs).reduce((acc, [id, job]) => {
    if (!acc[job.status]) acc[job.status] = {};
    acc[job.status][id] = job;
    return acc;
  }, {todo: {}, running: {}, done: {}, failed: {}});

  if (loading) {
    return <div className="flex h-screen bg-gray-900 text-white items-center justify-center">Loading...</div>;
  }

  if (error) {
    return <div className="flex h-screen bg-gray-900 text-white items-center justify-center">{error}</div>;
  }

  return (
    <div className="flex flex-col h-screen bg-gray-900 text-white">
      <ProgressBar jobsByStatus={jobsByStatus} totalETA={totalETA} />
      <div className="flex-1 flex flex-row overflow-hidden">
        <StatusColumn title="To Do" jobs={jobsByStatus.todo} />
        <StatusColumn title="Running" jobs={jobsByStatus.running} />
        <StatusColumn title="Done" jobs={jobsByStatus.done} />
        <StatusColumn title="Failed" jobs={jobsByStatus.failed} />
      </div>
    </div>
  );
};

const App = () => {
  return (
    <div className="App">
      <Dashboard />
    </div>
  );
}

export default App;

import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import configData from "../config.json";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

const CodeLinesGraph = ({linesCount, hideZero}) => {
  const options = {
      indexAxis: 'y',
      elements: {
          bar: {
              borderWidth: 2,
          },
      },
      responsive: true,
      scales: {
          y: {
              min: 0,
              max: 50
          }
      },
      plugins: {
        legend: {
            display: false,
        },
      title: {
          display: true,
          text: 'Gazes per Line',
          },
      },
  }

  const labels = Object.keys(linesCount);
  const mockData = Object.values(linesCount);
  if (hideZero) {
    labels.shift();
    mockData.shift();
  }

  const data = {
    labels,
    datasets: [
      {
        data: mockData,
        borderColor: configData.THEME_COLORS.PRIMARY_RGB,
        backgroundColor: configData.THEME_COLORS.PRIMARY_BG_RGB,
      },
    ],
  };

  return <Bar options={options} data={data} />
  }
  
export default CodeLinesGraph
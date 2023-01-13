import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import configData from "../config.json";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title, 
  Tooltip,
  Legend
);

const LhipaGraph = ({labels, data}) => {
    const options = {
        responsive: true,
        plugins: {
          legend: {
            display: false,
          },
          title: {
            display: true,
            text: 'LHIPA over Time',
          },
        },
      };
    
    const dataObject = {
      labels,
      datasets: [
          {
          data,
          cubicInterpolationMode: 'monotone',
          borderColor: configData.THEME_COLORS.PRIMARY_RGB,
          backgroundColor: configData.THEME_COLORS.PRIMARY_BG_RGB,
          tension: 0.7
          },
      ],
    };

    return <Line options={options} data={dataObject} />
}

export default LhipaGraph
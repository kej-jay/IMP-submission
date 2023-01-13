import React from 'react'
import { Chart as ChartJS, RadialLinearScale, ArcElement, Tooltip, Legend } from 'chart.js';
import { PolarArea } from 'react-chartjs-2'
import configData from "../config.json";

ChartJS.register(RadialLinearScale, ArcElement, Tooltip, Legend);

const IrisGraph = ({identifier, value}) => {
  const data = {
    labels: [identifier,],
    datasets: [
      {
        label: 'Pupil Diameter',
        data: [value],
        borderColor: configData.THEME_COLORS.PRIMARY_RGB,
        backgroundColor: configData.THEME_COLORS.PRIMARY_BG_RGB,
      },
    ],
  };

  const options =  {
    scales: {
        r: {
            min: 0,
            max: 7
        }
    },
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: identifier + ' pupil',
      },
    }
  }
  
  return (
    <div>
      <PolarArea data={data} options={options}/>
    </div>
  )
}

export default IrisGraph
import React, { useState } from 'react'

import WebSocketConnect from './components/WebSocketConnect'
import PolarArea from './components/IrisGraph'
import LhipaGraph from './components/LhipaGraph'
import CodeLinesGraph from './components/CodeLinesGraph'
import './App.css'

const App = () => {
  const [leftPupilSize, setLeftPupilSize] = useState(0);
  const [rightPupilSize, setRightPupilSize] = useState(0);
  const pupilAdjust = (left, right) => {
      setLeftPupilSize(left);
      setRightPupilSize(right);
  }  
  const [lhipaLabel, setLhipaLabel] = useState([]);
  const [lhipaData, setLhipaData] = useState([]);
  const updateLhipa = (timestamp, data) => {
    setLhipaLabel(lhipaLabel => [...lhipaLabel, timestamp])
    setLhipaData(lhipaData => [...lhipaData, data])
  }    

  const [linesCount, setLinesCount] = useState(() => {
    let filledArray = new Array(50).fill(0);
    return {...filledArray}
  }); 
  let updateLineCount = (line, value) => {
    setLinesCount(prevState => {
      return {...prevState, ...{[line]: Math.max(value, linesCount[line])}};
    });
  }    
  const [hideZero, setHideZero] = useState(false);

  return (
    <div>
      <WebSocketConnect updatePupilSizes={pupilAdjust} lhipaUpdate={updateLhipa} linesCountUpdate={updateLineCount}/>
      <div style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center"
        }}>
            <PolarArea identifier="left" value={leftPupilSize}/>
            <PolarArea identifier="right" value={rightPupilSize}/>
      </div>    
      
      <LhipaGraph 
        labels={lhipaLabel} 
        data={lhipaData}/>

      <button onClick={() => {setHideZero(!hideZero)}} >
          Hide Zeros
      </button>
      <CodeLinesGraph linesCount={linesCount} hideZero={hideZero} />
    </div>
  )
}

export default App

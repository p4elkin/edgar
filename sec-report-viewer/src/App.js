import React from "react";
import "./App.css";
import Navbar from "./components/Navbar";
import { BrowserRouter as Router, Switch, Route } from "react-router-dom";
import About from "./pages/About";
import Home from "./pages/Home";
import {FilingCard} from "./components/FilingCard";
function App() {
  return (
      <FilingCard/>
  );
}

export default App;

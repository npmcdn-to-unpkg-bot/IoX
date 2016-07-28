(function(baseUrl, modElement) {
  var makeDataset = (label, color, map) => (data) => (
    {
      label: label,
      fill: false,
      lineTension: 0.1,
      backgroundColor: color,
      borderColor: color,
      borderCapStyle: 'butt',
      borderDash: [],
      borderDashOffset: 0.0,
      borderJoinStyle: 'miter',
      pointBorderColor: color,
      pointBackgroundColor: '#fff',
      pointBorderWidth: 1,
      pointHoverRadius: 5,
      pointHoverBackgroundColor: color,
      pointHoverBorderColor: 'rgba(220,220,220,1)',
      pointHoverBorderWidth: 2,
      pointRadius: 1,
      pointHitRadius: 10,
      data: map(data)
    }
  );

  var arrayDelta = key => array => {
    var r = [];
    for (var i = 1; i < array.length; i++)
      r.push(array[i][key] - array[i-1][key]);
    r.push(r[r.length-1]);
    return r;
  };

  var TimeScale = Chart.scaleService.getScaleConstructor('time')
  var RightTimeScale = TimeScale.extend({
    buildTicks: function() {
      TimeScale.prototype.buildTicks.call(this);
      this.ticks.shift(); // remove the first tick
    }
  });

  Chart.scaleService.registerScaleType(
    "rtime",
    RightTimeScale,
    Chart.scaleService.getScaleDefaults('time')
  );

  var ReactChart = React.createClass({
    render: function() {
      var redraw = ref => {
        if (ref == null)
          return;
        if (this.chart)
          this.chart.destroy();
        this.chart = new Chart(ref, {
          type: this.props.type,
          data: this.props.data,
          options: this.props.options
        });
      };
      return <canvas ref={redraw} height={this.props.height} width={this.props.width}/>;
    }
  });

  var Stats = React.createClass({
    getInitialState: function() {
      return {
        timestamps: [],
        data: []
      };
    },

    componentDidMount: function() {
      var collectData = Suave.EvReact.remoteCallback(
        baseUrl + 'stats',
        data => {
          this.state.timestamps.push(new Date());
          this.state.data.push(data);
          if(this.state.timestamps.length > 300) {
            this.state.timestamps.shift();
            this.state.data.shift();
          }
          this.setState(this.state);
        }
      );
      var touchState = () => { this.setState(this.state); };
      this.dataTimer = setInterval(collectData, 2000);
      //this.scrollTimer = setInterval(touchState, 200);
    },

    componentWillUnmount: function() {
      clearInterval(this.dataTimer);
      //clearInterval(this.scrollTimer);
    },

    render: function() {
      var now = Date.now();
      var options = {
        animation: {
          duration: 0
        },
        scales: {
          reverse: true,
          xAxes: [{
              type: 'rtime',
              time: {
                unit: 'minute',
                max: now,
                min: now - 300000,
              },
              gridLines: {
                color: 'rgba(0, 0, 0, 0.1)',
                zeroLineColor: 'rgba(0, 0, 0, 0.1)',
              },
              position: 'bottom'
          }],
          yAxes: [{
              type: 'linear',
              position: 'right'
          }]
        }
      };

      var labels = this.state.timestamps.slice(1);
      labels.push(new Date());

      var data = {
        labels: labels,
        datasets: this.props.fields.map(d => d(this.state.data))
      };

      return <ReactChart type="line" data={data} options={options} width="600" height="250"/>
    }
  });

  var Configuration = React.createClass({
    getInitialState: function() {
      return {
        Verbose: false
      };
    },

    getConfig: function() {
      Suave.EvReact.remoteCallback(
        baseUrl + 'getConfig',
        data => this.setState(data)
      )();
    },

    setConfig: function() {
      Suave.EvReact.remoteCallback(baseUrl + 'saveConfig')(this.state);
    },

    reloadConfig: function() {
      Suave.EvReact.remoteCallback(
        baseUrl + 'reloadConfig',
        data => this.getConfig(),
        true
      )();
    },

    componentDidMount: function() {
      this.getConfig();
    },

    render: function() {
      var renderField = id => {
        if (!(id in this.state))
          return;

        var props = {};
        if (typeof(this.state[id]) == "boolean") {
          props.type = "checkbox";
          props.onChange = event => this.setState({ [id]: event.target.checked });
          props.checked = this.state[id];
        } else {
          props.type = "text";
          props.onChange = event => this.setState({ [id]: event.target.value });
          props.value = this.state[id];
        }

        return (
          <div key={id} className="form-group row">
            <label className="col-sm-3 control-label text-right">{id}</label>
            <div className="col-sm-9">
              <input className="form-control" {...props} />
            </div>
          </div>
        );
      };

      return (
        <form className="form-horizontal">
          {this.props.fields.map(renderField)}
          <div className="row">
            <div className="col-sm-2" />
            <div className="col-sm-2">
              <input type="button" className="form-control" onClick={this.setConfig} value="Apply" />
            </div>
            <div className="col-sm-1" />
            <div className="col-sm-2">
              <input type="button" className="form-control" onClick={this.getConfig} value="Refresh" />
            </div>
            <div className="col-sm-1" />
            <div className="col-sm-2">
              <input type="button" className="form-control" onClick={this.reloadConfig} value="Reload" />
            </div>
            <div className="col-sm-2" />
          </div>
        </form>
      );
    }
  });

  var ConfStatModule = React.createClass({
    render: function() {
      return (
        <div>
          <ul className="nav nav-tabs" role="tablist">
            <li className="nav-item active">
              <a className="nav-link active" data-toggle="tab" href="#stats" role="tab">Statistics</a>
            </li>
            <li className="nav-item">
              <a className="nav-link" data-toggle="tab" href="#config" role="tab">Configuration</a>
            </li>
          </ul>
          <div className="tab-content">
            <div className="tab-pane active" id="stats" role="tabpanel"><Stats fields={this.props.statFields}/></div>
            <div className="tab-pane" id="config" role="tabpanel"><Configuration fields={this.props.configFields} /></div>
          </div>
        </div>
      );
    }
  });

  var DispatcherModule = React.createClass({
    render: function() {
      var configFields = [
        "DestinationHost",
        "DestinationPort",
        "Verbose"
      ];

      var statFields = [
        makeDataset("Incoming bytes/s", 'rgba(75,192,192,1)', arrayDelta("CompressedNetBytes")),
        makeDataset("Outgoing bytes/s", 'rgba(192,75,75,1)', arrayDelta("RawNetBytes"))
      ];

      return <ConfStatModule statFields={statFields} configFields={configFields} />;
    }
  });

  var CollectorModule = React.createClass({
    render: function() {
      var configFields = [
        "Destination",
        "ForwardPriorityThreshold",
        "DumpPriorityThreshold",
        "SyslogPort",
        "BufferSizeThreshold",
        "BufferTimeoutMS",
        "Verbose"
      ];

      var statFields = [
        makeDataset("Outgoing bytes/s", 'rgba(75,192,192,1)', arrayDelta("CompressedNetBytes")),
        makeDataset("Incoming bytes/s", 'rgba(192,75,75,1)', arrayDelta("RawDiskBytes"))
      ];

      return <ConfStatModule statFields={statFields} configFields={configFields} />;
    }
  });

  var Module = React.createClass({
    getInitialState: function() {
      return {
        modType: ""
      };
    },

    componentDidMount: function() {
      // Find out the type of module from the stats it provides
      Suave.EvReact.remoteCallback(
        baseUrl + 'stats',
        data => this.setState({ modType: "RawDiskBytes" in data ? "collector" : "dispatcher" })
      )();
    },

    render: function() {
      if (this.state.modType == "dispatcher")
        return <DispatcherModule />;
      else if (this.state.modType == "collector")
        return <CollectorModule />;
      else
        return <div />;
    }
  });

  ReactDOM.render(<Module/>, modElement);
})

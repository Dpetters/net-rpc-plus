Array.prototype.getUnique = function(){
  var u = {}, a = [];
  for(var i = 0, l = this.length; i < l; ++i){
    if(u.hasOwnProperty(this[i])) {
      continue;
    }
    a.push(this[i]);
    u[this[i]] = 1;
  }
  return a;
}

function counts(arr) {
  var res = [], prev;

  arr.sort();
  for ( var i = 0; i < arr.length; i++ ) {
    console.log(prev);
    if ( res.length==0 || arr[i] !== prev ) {
      res.push([arr[i], 1]);
    } else {
      res[res.length-1][1]++;
    }
    prev = arr[i];
  }
  return res;
}
var chart;

var getSeriesData = function() {
  var dataByServiceMethod = JSONQuery("[?ServiceMethod='" + $("#message-types").val() + "'][=DestinationAddress]", data);
  return counts(dataByServiceMethod);
}

$(document).ready(function(){
  var messageTypes = JSONQuery("[=ServiceMethod]", data).getUnique()

  $("#message-types").change(function(e){
    console.log("hi");
    chart.series[0].setData(getSeriesData(), true);
  });
  
  for(i = 0; i < messageTypes.length; i++){
    $("#message-types").append("<option value=\"" + messageTypes[i] + "\">" + messageTypes[i] + "</option>");
  }

  chart = new Highcharts.Chart({
    chart: {
      renderTo:'container',
    },
    plotOptions: {
      pie: {}
    },
    series: [{
      type:'pie',
      data: getSeriesData()
    }]
  });
});

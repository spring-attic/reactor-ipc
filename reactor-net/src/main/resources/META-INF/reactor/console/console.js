/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Created by Alex on 5/20/2015.
 */

function loadJSON(path, success, error) {
    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4) {
            if (xhr.status === 200) {
                success(JSON.parse(xhr.responseText));
            }
            else {
                error(xhr);
            }
        }
    };
    xhr.open('GET', path, true);
    xhr.send();
}


function getScaleFreeNetwork(nodeCount) {

    var nodes = [];
    var edges = [];
    var connectionCount = [];

    // randomly create some nodes and edges
    for (var i = 0; i < nodeCount; i++) {
        nodes.push({
            id: i,
            label: String(i)
        });

        connectionCount[i] = 0;

        // create edges in a scale-free-network way
        if (i == 1) {
            var from = i;
            var to = 0;
            edges.push({
                from: from,
                to: to
            });
            connectionCount[from]++;
            connectionCount[to]++;
        }
        else if (i > 1) {
            var conn = edges.length * 2;
            var rand = Math.floor(Math.random() * conn);
            var cum = 0;
            var j = 0;
            while (j < connectionCount.length && cum < rand) {
                cum += connectionCount[j];
                j++;
            }


            var from = i;
            var to = j;
            edges.push({
                from: from,
                to: to
            });
            connectionCount[from]++;
            connectionCount[to]++;
        }
    }

    return {nodes:nodes, edges:edges};
}

var randomSeed = 764; // Math.round(Math.random()*1000);
function seededRandom() {
    var x = Math.sin(randomSeed++) * 10000;
    return x - Math.floor(x);
}
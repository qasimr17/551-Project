import { Component, OnInit, Inject } from '@angular/core';
import { faFile, faFolder, faAngleLeft, faHome, faAdd, faSearch, faX } from '@fortawesome/free-solid-svg-icons';
import { MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { AddFolderComponent } from './add-folder/add-folder.component';
import { ApiService } from '../app/services/api.service';
import { AddFileComponent } from './add-file/add-file.component';
import { SearchComponent } from './search/search.component';
import { ViewPartitionComponent } from './view-partition/view-partition.component';
import {RemoveFileComponent} from './remove-file/remove-file.component';
import {NgxSpinnerService} from 'ngx-spinner'


interface Children {
  name: String,
  folder: boolean
}

interface Database{
  name: String,
  value: String
}

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})


export class AppComponent implements OnInit {
  children: String[] = [];
  selectedDatabase = '';
  databases: Database[] = [{name: 'MySQL', value: 'sql'},{name: 'MongoDB', value: 'mongodb'}];
  faFile = faFile;
  faFolder = faFolder;
  faAngleLeft = faAngleLeft;
  faHome = faHome;
  faAdd = faAdd;
  faSearch = faSearch;
  faX = faX;
  currentPath = '';
  addFolder = false;
  showFile = false;
  columns: any[] = [];
  data: any;
  jsonData: any;
  removeFile = false;

  constructor(public matDialog: MatDialog, public api: ApiService, public spinner: NgxSpinnerService) { }

  ngOnInit() {
    this.currentPath = '/';
    this.selectedDatabase = 'sql';
    this.api.currentPath = this.currentPath;
    this.api.database = this.selectedDatabase;
    
    //call ls of root at start
    this.api.ls(this.currentPath).subscribe((response: any) => {
      this.spinner.show();
      this.children = response['files'];
      this.api.children = this.children;
      this.spinner.hide();
    });
    // this.children = ['Tejas', 'Zainab', 'Qasim', 'Hello.csv'];
  }

  changeDB() {
    this.api.database = this.selectedDatabase;
    this.api.ls(this.currentPath).subscribe((response: any) => {
      this.spinner.show();
      this.children = response['files'];
      this.api.children = this.children;
      this.spinner.hide();
    });

  }

  explore(name: String) {
    if(!name.split('.')[1]){
      if(this.currentPath.length>1){
        this.currentPath = this.currentPath + '/';
      }
      this.currentPath = this.currentPath + name;
      this.api.currentPath = this.currentPath;
      //call ls of name
      this.api.ls(this.currentPath).subscribe((response: any) => {
        this.spinner.show();
        this.children = response['files'];
        this.api.children = this.children;
        this.spinner.hide();
      });
      
      // if(name == 'Tejas'){
  
      //   this.children = ['Cars.csv', 'User'];
  
      // }
    }
   
    else{
      //call cat function
      this.cat(name)
    }
  }
  back() {
    let path = this.currentPath.split('/');
    let newPath = '';
    for (let x = 0; x < path.length - 1; x++) {
      newPath = newPath + path[x]+ '/';
    }
    if(newPath.length != 1){
      this.currentPath = newPath.slice(0, -1);
    }
    else{
      this.currentPath = newPath;
    }
    console.log(this.currentPath)
    this.api.currentPath = this.currentPath
    //call ls of currentPath
    // this.children = ['Tejas', 'Zainab', 'Qasim', 'Hello.csv'];
    this.api.ls(this.currentPath).subscribe((response: any) => {
      this.spinner.show();
      this.children = response['files'];
      this.showFile = false;
      this.data = null;
      this.jsonData = {};
      this.columns = [];
      this.api.columns = [];
      this.api.children = this.children;
      console.log("On Back Data:" + this.data);
      this.spinner.hide();
    });
  }

  // openAddFolder(){
  //   console.log('openAddFolder called');
  //   this.addFolder= true;
  // }
  openAddFolder() {
    const dialogConfig = new MatDialogConfig();
    // The user can't close the dialog by clicking outside its body
    dialogConfig.disableClose = true;
    dialogConfig.id = "modal-component";
    dialogConfig.height = "350px";
    dialogConfig.width = "600px";
    // https://material.angular.io/components/dialog/overview
    const modalDialog = this.matDialog.open(AddFolderComponent, dialogConfig);
    modalDialog.afterClosed().subscribe(()=>{
      this.api.ls(this.currentPath).subscribe((response: any) => {
        this.spinner.show();
        this.children = response['files'];
        this.api.children = this.children;
        this.spinner.hide();
      })
    });
  }

  openAddFile() {
    const dialogConfig = new MatDialogConfig();
    // The user can't close the dialog by clicking outside its body
    dialogConfig.disableClose = true;
    dialogConfig.id = "modal-component";
    dialogConfig.height = "350px";
    dialogConfig.width = "600px";
    // https://material.angular.io/components/dialog/overview
    const modalDialog = this.matDialog.open(AddFileComponent, dialogConfig);
    modalDialog.afterClosed().subscribe(()=>{
      this.api.ls(this.currentPath).subscribe((response: any) => {
        this.spinner.show();
        this.children = response['files'];
        this.api.children = this.children;
        this.spinner.hide();
      });
    });
  }

  openRemoveFile(){
    const dialogConfig = new MatDialogConfig();
    // The user can't close the dialog by clicking outside its body
    dialogConfig.disableClose = true;
    dialogConfig.id = "modal-component";
    dialogConfig.height = "350px";
    dialogConfig.width = "600px";
    // https://material.angular.io/components/dialog/overview
    const modalDialog = this.matDialog.open(RemoveFileComponent, dialogConfig);
    modalDialog.afterClosed().subscribe(()=>{
      this.api.ls(this.currentPath).subscribe((response: any) => {
        this.spinner.show();
        this.children = response['files'];
        this.api.children = this.children;
        this.spinner.hide();
      });
    });
  }

  openSearch() {
    const dialogConfig = new MatDialogConfig();
    // The user can't close the dialog by clicking outside its body
    dialogConfig.disableClose = true;
    dialogConfig.id = "modal-component";
    dialogConfig.minHeight = "350px";
    dialogConfig.maxHeight = "700px"
    dialogConfig.minWidth = "350px";
    dialogConfig.maxWidth = "90%";
    // https://material.angular.io/components/dialog/overview
    const modalDialog = this.matDialog.open(SearchComponent, dialogConfig);
  }

  openViewPartition() {
    const dialogConfig = new MatDialogConfig();
    // The user can't close the dialog by clicking outside its body
    dialogConfig.disableClose = true;
    dialogConfig.id = "modal-component";
    dialogConfig.minHeight = "350px";
    dialogConfig.maxHeight = "700px"
    dialogConfig.minWidth = "350px";
    dialogConfig.maxWidth = "90%";
    // https://material.angular.io/components/dialog/overview
    const modalDialog = this.matDialog.open(ViewPartitionComponent, dialogConfig);
    
    //this.displaySearchData();
  }

  cat(name: String) {
    this.currentPath = this.currentPath == '/' ? this.currentPath + name : this.currentPath + '/' + name
    console.log('In cat'+ this.currentPath)
    this.api.currentPath = this.currentPath;
    this.data = null;
    this.jsonData = null;
    this.api.cat(this.currentPath).subscribe((response: any) =>{
      this.spinner.show();
      this.jsonData = response;
      for (let x of this.jsonData['schema']['fields']) {
        this.columns.push(x['name']);
      }
      console.log(this.columns)
      this.data = this.jsonData['data'];
      this.showFile = true;
      this.api.columns = this.columns;
      this.spinner.hide();
    });
  }

  displaySearchData(data: String) {
    this.data = null;
    this.jsonData = JSON.parse(this.api.catResponse);
    console.log(this.jsonData);
    for (let x of this.jsonData['schema']['fields']) {
      this.columns.push(x['name']);
    }
    console.log(this.columns)
    this.data = this.jsonData['data'];
    this.showFile = true;
    this.currentPath = this.currentPath == '/' ? this.currentPath + name : this.currentPath + '/' + name;
    this.api.currentPath = this.currentPath;
    this.api.columns = this.columns;
  }

  
}

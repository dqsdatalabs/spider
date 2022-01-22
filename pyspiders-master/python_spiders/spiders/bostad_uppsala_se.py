
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest 
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
 

class MySpider(Spider):
    name = 'bostad_uppsala_se'
    execution_type='testing'
    country='swenden'
    locale='sv'
    external_source = "Bostaduppsalase_Pyspider_sweden"
    start_url = "https://www.bostad.uppsala.se/mypages/api"
    custom_settings = {

        'HTTPCACHE_ENABLED':False,

    }


    headers = {
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    }


    def start_requests(self):    

        payload = {"query": "query getRentalObjectsAvailable {\n  getRentalObjectsAvailable {\n    regionNames\n    districtNames\n    regionDistrictNames\n    landlordNames\n    allLandlordNames\n    allRegionDistrictNames\n    allRegionNames\n    allBoendeTyp {\n      rentalObjectCategoryId\n      name\n    }\n    allOvrigt {\n      rentalObjectCategoryId\n      name\n    }\n    rentalObjects {\n      rooms\n      area\n      rentalObjectId\n      balcony\n      elevator\n      street\n      rent\n      rentFormated\n      rentDescription\n      regionName\n      region {\n        regionId\n      }\n      district {\n        districtId\n      }\n      districtName\n      imagePrimaryId\n      imagePrimaryCdn\n      startDate\n      startDateFormated\n      endDate\n      endDateFormated\n      moveInDate\n      moveInDateFormated\n      landlord\n      landlordId\n      projectName\n      latitude\n      longitude\n      queueTypeId\n      queueDefault\n      queueTenant\n      queueTransfer\n      boendeTyp {\n        rentalObjectCategoryId\n        name\n      }\n      bostadsTyp {\n        rentalObjectCategoryId\n        name\n      }\n      kontraktsTyp {\n        rentalObjectCategoryId\n        name\n      }\n      formedlingsTyp {\n        rentalObjectCategoryId\n        name\n      }\n      fastighetsStatus {\n        rentalObjectCategoryId\n        name\n      }\n      rentalObjectCategories {\n        rentalObjectCategoryId\n        name\n      }\n    }\n  }\n}\n"}
        yield Request(self.start_url,
                    method="POST",
                    callback=self.parse,
                    body=json.dumps(payload),
                    headers=self.headers)



    def parse(self, response):
        data_all = json.loads(response.body)
        data = data_all["data"]["getRentalObjectsAvailable"]["rentalObjects"]
        for item in data:
            house_id = item["rentalObjectId"]
            url = f"https://www.bostad.uppsala.se/mypages/app/visa/{house_id}"
            yield Request(url, callback=self.populate_item,meta={"item":item})


    def populate_item(self, response):
        

        item_loader = ListingLoader(response=response)
        item = response.meta.get("item")
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_value("title",item["rentDescription"])
        item_loader.add_value("room_count",item["rooms"])
        item_loader.add_value("square_meters",item["area"])
        item_loader.add_value("external_id",str(item["rentalObjectId"]))
        im=str(item['imagePrimaryId'])
        if im:
            item_loader.add_value("images",f"https://ubf-prod-mypages.azureedge.net/mypages/image/{im}")
        item_loader.add_value("latitude",item["latitude"])
        item_loader.add_value("longitude",item["longitude"])
        item_loader.add_value("property_type",item["bostadsTyp"][0]["rentalObjectCategoryId"])
        item_loader.add_value("landlord_name",item["landlord"])
        item_loader.add_value("city",item["districtName"])
        item_loader.add_value("available_date",item["moveInDateFormated"])
        if item["balcony"] == True:
            item_loader.add_value("balcony",True)

        if item["elevator"] == True:
            item_loader.add_value("elevator",True)


        rent = str(item["rent"]).split(".")[0]
        item_loader.add_value("rent",rent)

        address = item["street"] + "-" + item["districtName"]
        item_loader.add_value("address",address)

        payload2 = {"query":"query getRentalObject($rentalObjectId: Long!) {\n  getRentalObject(rentalObjectId: $rentalObjectId) {\n    rentalObjectId\n    objectNumber\n    apartmentNumber\n    street\n    city\n    zipcode\n    latitude\n    longitude\n    posSystem\n    description\n    rooms\n    roomsDescription\n    floor\n    floorTotal\n    rent\n    rentFormated\n    rentDescription\n    area\n    maxNumberOfPeople\n    elevator\n    balcony\n    startDate\n    startDateFormated\n    endDate\n    endDateFormated\n    moveInDate\n    moveInDateFormated\n    showing\n    showingIsMandatory\n    lastReplyDate\n    moveInDate\n    moveInDateFormated\n    moveInEarlier\n    applicationCount\n    keepQueueDate\n    projectId\n    projectName\n    districtName\n    regionName\n    queueTypeId\n    queueDefault\n    queueTenant\n    queueTransfer\n    district {\n      name\n      districtId\n      region {\n        name\n        regionId\n      }\n    }\n    landlord {\n      landlordId\n      name\n    }\n    imagePrimary {\n      imageId\n      description\n      cdn\n    }\n    imageOthers {\n      imageId\n      description\n      cdn\n    }\n    imageLayouts {\n      imageId\n      description\n      cdn\n    }\n    rentalObjectProperties {\n      rentalObjectPropertyId\n      key\n      ordinal\n      rentalObjectPropertyValues {\n        rentalObjectPropertyValueId\n        value\n        type\n        ordinal\n      }\n    }\n    rentalObjectCriterias {\n      rentalObjectCriteriaId\n      text\n      textLong\n    }\n    rentalObjectPredefinedCriterias {\n      rentalObjectCriteriaId\n      text\n      textLong\n    }\n    rentalObjectPredefinedAcceptCriterias {\n      rentalObjectCriteriaId\n      text\n      textLong\n    }\n    incomeTypes {\n      name\n      description\n      incomeCategory\n    }\n    boendeTyp {\n      rentalObjectCategoryId\n      name\n    }\n    bostadsTyp {\n      rentalObjectCategoryId\n      name\n    }\n    kontraktsTyp {\n      rentalObjectCategoryId\n      name\n    }\n    formedlingsTyp {\n      rentalObjectCategoryId\n      name\n    }\n    fastighetsStatus {\n      rentalObjectCategoryId\n      name\n    }\n    agencyFee {\n      currency\n      value\n    }\n    siteDescription\n  }\n}\n","variables":{"rentalObjectId":f"{item['rentalObjectId']}"}}
        item_loader.add_value("landlord_phone", "0771- 71 00 00")
        item_loader.add_value("landlord_email", "info@bostad.uppsala.se")
        item_loader.add_value("currency","SEK")

        yield Request(self.start_url,
                    method="POST",
                    callback=self.take_rest,
                    body=json.dumps(payload2),
                    headers=self.headers,
                    meta={"itemloader":item_loader})


    def take_rest(self, response):
        
        
        data_all = json.loads(response.body)
        data = data_all["data"]["getRentalObject"]
        
        
        item_loader = response.meta.get("itemloader")
  
        item_loader.add_value("zipcode",data["zipcode"])
        item_loader.add_value("description",data["description"])
        item_loader.add_value("floor",str(data["floor"]))

        item_loader.add_value("title",data["rentDescription"])

        titlecheck=item_loader.get_output_value("title")
        if not titlecheck:
            item_loader.add_value("title",data["city"])
       
        image_base = "https://ubf-prod-mypages.azureedge.net/mypages/image/"
        images = []
        primary_img = image_base + str(data["imagePrimary"]["imageId"])
        images.append(primary_img)
        if data.get("imageOthers"):
            for img in data["imageOthers"]:
                img_url = image_base + str(img["imageId"])
                images.append(img_url)
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        yield item_loader.load_item() 
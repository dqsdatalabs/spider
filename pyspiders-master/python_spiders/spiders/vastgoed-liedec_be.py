# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'vastgoed-liedec_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Vastgoedliedec_PySpider_belgium'
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }
    headers={
            "content-type": "application/json",
            "cookie": "cookiebarDisplay=CLOSED",
            "origin": "https://vastgoed-liedec.be",
            "referer": "https://vastgoed-liedec.be/te-huur/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
    
    def start_requests(self):
        start_urls = [
            {
                "type" : "1",
                "property_type" : "apartment"
            },
            {
                "type" : "3",
                "property_type" : "house"
            },   
        ]

        for url in start_urls:
            cod_tipologia = str(url.get("type"))
            payload={"Transaction":2,"Type":cod_tipologia,"City":"0","MinPrice":"0","MaxPrice":"0","MinSurface":"0","MaxSurface":"0","MinSurfaceGround":"0","MaxSurfaceGround":"0","MinBedrooms":"0","MaxBedrooms":"0","Radius":"0","NumResults":"18","StartIndex":1,"ExtraSQL":"0","ExtraSQLFilters":"0","NavigationItem":"0","PageName":"0","Language":"NL","CountryInclude":"0","CountryExclude":"0","Token":"QOLMNUPDMBYIFBXTGBXFKGMQXVWYOCULYILFOCWMRSGAWEWWKD","SortField":"1","OrderBy":1,"UsePriceClass":False,"PriceClass":"0","SliderItem":"0","SliderStep":"0","CompanyID":"0","SQLType":"3","MediaID":"0","PropertyName":"0","PropertyID":"0","ShowProjects":False,"Region":"0","currentPage":"0","homeSearch":"0","officeID":"0","menuIDUmbraco":"0","investment":False,"useCheckBoxes":False,"CheckedTypes":"0","newbuilding":False,"bedrooms":0,"latitude":"0","longitude":"0","ShowChildrenInsteadOfProject":False,"state":"0","FilterOutTypes":"0"}
            mainurl="https://vastgoed-liedec.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0"
            if url:
                yield Request(mainurl,callback=self.parse,body=json.dumps(payload),method="POST",headers=self.headers,meta ={"property_type":url.get("property_type")})


    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)
        for item in data:
            type=str(item['Property_SEO']).strip().split(" ")[0].lower()
            id=str(item['Property_URL']).split("/0/")[0]
            country=str(item['Property_SEO']).strip().split(" ")[-1].lower()
            url=f"https://vastgoed-liedec.be/nl/te-huur/{country}/{type}/{id.split('/')[-1]}/"
            yield Request(url, callback=self.populate_item,meta={'item':item,'property_type':response.meta.get("property_type")})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item=response.meta.get("item")
        external_id=item['FortissimmoID']
        if external_id:
            item_loader.add_value("external_id",external_id)
        city=item['Property_City_Value']
        if city:
            item_loader.add_value("city",city)
        description=item['Property_Description']
        if description:
            item_loader.add_value("description",description)
        latitude=item['Property_Lat']
        if latitude:
            item_loader.add_value("latitude",str(latitude))
        longitude=item['Property_Lon']
        if longitude:
            item_loader.add_value("longitude",str(longitude))
        title=item['Property_Title']
        if title:
            item_loader.add_value("title",title)
        zipcode=item['Property_Zip']
        if title:
            item_loader.add_value("zipcode",zipcode)
        room_count=item['bedrooms']
        if room_count:
            item_loader.add_value("room_count",room_count)
        rent=response.xpath("//p[.='Prijs :']/parent::th/following-sibling::td/p/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        
        images=[x for x in response.xpath("//img[contains(@src,'fortissimmo')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
            
        adres="".join(response.xpath("//div[@class='detail_adres']//p//text()").getall())
        if adres:
            item_loader.add_value("address",adres.replace("\r","").replace("\n","").strip())
        square_meters=response.xpath("//p[.='Bewoonbare opp. :']/parent::th/following-sibling::td/p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0].strip())
        energy_label=response.xpath("//p[.='EPC :']/parent::th/following-sibling::td/p/text()").get()
        if energy_label:
            energy = energy_label.replace("(","").replace(")","").split("k")[0]
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        elevator=response.xpath("//p[.='Lift :']/parent::th/following-sibling::td/p/text()").get()
        if elevator and elevator=="Ja":
            item_loader.add_value("elevator",True)

        item_loader.add_value("landlord_name","Vastgoed Liedec")
        item_loader.add_value("landlord_email","+32 9 328 49 45")
        item_loader.add_value("landlord_phone","info@vastgoed-liedec.be")


        yield item_loader.load_item() 
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label

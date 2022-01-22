# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 
from bs4 import BeautifulSoup 
class MySpider(Spider):
    name = 'svenbo_com'
    execution_type = 'testing'
    country = 'sweden'  
    locale = 'sv'  
    external_source="Svenbo_PySpider_sweden"

    def start_requests(self):
        
   
        p_url ="https://www.svenbo.com/rentalobject/Listapartment/published?sortOrder=NEWEST&timestamp=1636702826054"
        yield Request(
            p_url,
            callback=self.parse,

           
        )    

    # 1. FOLLOWING
    def parse(self, response):

        data=json.loads(response.body)["data"].replace("\\","")
        lenimage="||".join(data.split('FirstImage'))
        b=lenimage.replace("['","").replace("']","")
        lent=data.split('DetailsUrl')
        a=len(lent)
        i=0
        while i<a-1:
            follow_url = response.urljoin(json.loads(data)[i]["DetailsUrl"])
            imager=str(b.split("||")[i+1]).split("Guid")[1].split(",")[0]
            adres=json.loads(data)[i]["Adress1"]
            yield Request(follow_url, callback=self.populate_item,meta={"adres":adres,"image":imager})
            i=i+1

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
                
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type","house")

        rent=response.xpath("//div[@class='row']//div[@class='object-preview-headline-cc pb-2 pt-4 ']//div[@class='col-12 d-flex']/div/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("kr")[0].replace(" ",""))
        item_loader.add_value("currency", "SEK")
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        city=response.xpath("//span[contains(.,'Ort')]/text()").get()
        if city:
            item_loader.add_value("city",city.split(":")[-1].strip())

        features=response.xpath("//span[@class='object-preview-description-label-cc']//text()").getall()
        for i in features:
            if "Tillgänglig" in i:
                item_loader.add_value("available_date",i.split(":")[-1])
            if "Objektnr" in i:
                item_loader.add_value("external_id",i.split(":")[-1].strip())
        description=" ".join(response.xpath("//h4[.='Område']/following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        square_meters=response.xpath("//div[@class='mb-0 header3-font']/sup[.='2']/parent::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        room_count=response.xpath("//span[contains(.,'rum')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("rum")[0].strip())
        adres=response.meta.get("adres")
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("landlord_email"," info@svenbo.com")
        item_loader.add_value("landlord_phone"," 0325 - 61 90 90")
        image=response.meta.get("image")
        if image:
            image=image.replace(":","").replace('"',"").replace("Guid","").replace("[{","")
            item_loader.add_value("images",f"https://www.svenbo.com/Content/Image/{image}/0/0/True")
        item_loader.add_value("landlord_name","SVENBO")




        yield item_loader.load_item()
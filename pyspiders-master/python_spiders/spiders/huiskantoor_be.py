# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'huiskantoor_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Huiskantoor_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://huiskantoor.be/nl/huren?type=APARTMENT",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://huiskantoor.be/nl/huren?type=HOUSE",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='col-12 col-sm-6 estate']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\n","").replace(" ",""))        
        external_id="".join(response.url)
        if external_id:
            external_id = "".join(external_id.split("-")[-2:-1])
            item_loader.add_value("external_id", external_id)  
         
        address = response.xpath("(//div[@id='descrText']//text()[contains(.,'-')])[1]").get()
        if address:
            item_loader.add_value("address",address)   
            zipcode = address.split(" ")[1:2]
            city = address.split("-")[1]   
            item_loader.add_value("zipcode",zipcode)    
            item_loader.add_value("city",city) 

        room_count = response.xpath("//span[@class='property-stats_title']//text()[contains(.,'slaapkamers')]").get()
        if room_count:
            room_count= room_count.split("slaapkamers")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[@class='property-stats_title']//text()[contains(.,'badkamer')]").get()
        if bathroom_count:
            bathroom_count= bathroom_count.split("badkamer")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        rent =response.xpath("//h2//text()[contains(.,'€')]").get()
        if rent:     
            item_loader.add_value("rent",rent.split("€")[1])  
        else:
            rent =response.xpath("//div[@id='descrText']//text()[contains(.,'Huurprijs')]").get()
            if rent: 
                if "euro" in rent.lower():
                    rent = rent.split("Huurprijs :")[1].split("euro")[0]  
                    item_loader.add_value("rent",rent)
                elif "€" in rent:
                    rent = rent.split("Huurprijs € ")[1].split(" ")[0]   
                    item_loader.add_value("rent",rent) 
        item_loader.add_value("currency","EUR")  

        utilities =response.xpath("//div[@id='descrText']//text()[contains(.,'kosten bedragen')]").get()
        if utilities:     
            utilities = utilities.split("€")[1]
            utilities = utilities.split("/maand")[0]
            if " " in utilities:
                utilities = utilities.split(" / maand")[0]
            item_loader.add_value("utilities", utilities)  
        
        square =response.xpath("//div[@id='descrText']//text()[contains(.,'m²')]").get()
        if square:
            if "(" in square:
                square_meters =  square.split(" (")[1].split("m²")[0]
                item_loader.add_value("square_meters",square_meters)
        else:
            square =response.xpath("//span[@class='property-stats_title']//text()[contains(.,'woonopp')]").get()
            if square:
                square_meters =  square.split("m²")[0]
                item_loader.add_value("square_meters",square_meters)


        energy_label =response.xpath("//img[@class='energy-label']//@alt").get()    
        if energy_label:
            if "/" in energy_label:
                energy_label = energy_label. split("/")[1]
                item_loader.add_value("energy_label",energy_label)  
            else:
                item_loader.add_value("energy_label",energy_label)  

        parking =response.xpath("//table[@class='table-responsive']//th//text()[contains(.,'Garage')]").get()    
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        terrace =response.xpath("//table[@class='table-responsive']//th//text()[contains(.,'Terras')]").get()    
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        desc = " ".join(response.xpath("//div[@id='descrText']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date=response.xpath("//div[@id='descrText']//text()[contains(.,'/20')]").get()
        if available_date:
            available_date= available_date.split("Beschikbaar vanaf ")[1]
            available_date= available_date.split(".")[0]
            item_loader.add_value("available_date", available_date)
              
        images = [response.urljoin(x) for x in response.xpath("//img[@class='property-image-slider-image']//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//div[@class='map']//@data-location").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude":')[1].split(',"')[0]
            longitude = latitude_longitude.split('"longitude":')[1].split('}')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Het Huiskantoor")
        item_loader.add_xpath("landlord_phone", "//tr[td[contains(.,'Tel. nr')]]/td[2]//text()[normalize-space()]")
        item_loader.add_value("landlord_email", "info@huiskantoor.be")
        yield item_loader.load_item()
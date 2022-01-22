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
    name = 'agenceibaia_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agenceibaia_PySpider_france' 
    thousand_separator = ','
    scale_separator = '.'       
    
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.agenceibaia.com/location+immobilier.html",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[contains(@class,'more')]//a[contains(@class,'visited')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split('?')[0])
        item_loader.add_value("external_source", self.external_source)

        property_type=response.url
        if property_type and "maison" in property_type:
            item_loader.add_value("property_type","house")


        title = " ".join(response.xpath("//h1/span//text()").getall())
        if title:
            item_loader.add_value("title",title)
        address = response.xpath("//h2/span[@class='line-2']//text()").get()
        if address:
            address = address.split("/")[0].strip()
            item_loader.add_value("address", address)
            if " - " in address:
                item_loader.add_value("zipcode", address.split(" - ")[-1].strip())
                item_loader.add_value("city", address.split(" - ")[0].strip())
        external_id = " ".join(response.xpath("//div/p[contains(.,'Réf. :')]//text()").getall())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())    
           
        rent = response.xpath("//li[@class='li-price']//text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))     

        deposit = response.xpath("//div//p[contains(.,'Dépôt de garantie :')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(".","")
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))     
        
        square_meters = response.xpath("//li[contains(.,'Surface')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("Surface")[1].split(" m")[0].replace("de","").strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))     

        room_count = response.xpath("//li[contains(.,'Chambre')]//text()").get()
        if room_count:        
            item_loader.add_value("room_count",room_count.split("Chambre")[0].strip())

        bathroom_count = response.xpath("//li[contains(.,'Salle d')]//text()").get()
        if bathroom_count:        
            item_loader.add_value("bathroom_count",bathroom_count.split("Salle d")[0].strip())

        energy_label = response.xpath("//li//a[contains(.,'DPE :')]/span/text()").get()
        if energy_label:        
            energy_label = energy_label.split(":")[1].split("(")[0].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy_label)
     
        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath("//li[contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
 
        desc = " ".join(response.xpath("//div[contains(@class,'lo-details-intro')]//div[contains(@class,'lo-box-content')]/p[1]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())         
                    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='lo-image-inner-cadre']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "AGENCE IBAIA")
        item_loader.add_value("landlord_phone", "05.58.41.77.09")    
        available_date= response.xpath("//li[contains(.,'Date de disponibilité :')]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1], date_formats=["%m-%d-%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        yield item_loader.load_item()
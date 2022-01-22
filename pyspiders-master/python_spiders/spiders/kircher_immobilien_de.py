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
    name = 'kircher_immobilien_de'
    execution_type='testing'
    country = 'germany'
    locale ='de'
    external_source = "Kircher_Immobilien_PySpider_germany"
    start_urls = ["https://www.kircher-immobilien.de/objektangebote-kircher-immobilien.xhtml?p[obj0]=1"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='objectframe-list']//div[@class='listobject-information']"):
            p_type = item.xpath(".//tr[td[contains(.,'Objektart')]]/td[2]//text()").get()
            url = item.xpath(".//a[.='Details ansehen']/@href").get()
          
            if url and get_p_type_string(p_type):
                prop_type = get_p_type_string(p_type)
                follow_url = "https://www.kircher-immobilien.de/"+ url
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//tr[td[contains(.,'externe Objnr')]]/td[2]//text()")
     
        item_loader.add_value("external_source", self.external_source)    
        zipcode = response.xpath("//tr[td[contains(.,'PLZ')]]/td[2]//text()").get()      
        city = response.xpath("//tr[td[contains(.,'Ort')]]/td[2]//text()").get()    
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)

        if zipcode and city:
            item_loader.add_value("address", "{} {}".format(city,zipcode))

        item_loader.add_xpath("room_count", "//tr[td[contains(.,'Zimmer')]]/td[2]//text()")
        item_loader.add_xpath("bathroom_count", "//tr[td[contains(.,'Badezimmer')]]/td[2]//text()")

        rent = "".join(response.xpath("//tr[td[contains(.,'Kaltmiete')]]/td[2]//text()").getall()) 
        if rent:
            item_loader.add_value("rent_string", rent.replace(",00",""))
        square_meters = "".join(response.xpath("//tr[td[contains(.,'Wohnfl')]]/td[2]//text()[contains(.,'m')]").getall()) 
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(",")[0].split("m")[0])
        deposit = "".join(response.xpath("//tr[td[contains(.,'Kaution')]]/td[2]//text()").getall()) 
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",00","").replace(".",""))

        parking = response.xpath("//tr[td[contains(.,'Stellplatzart')]]/td[2]//text()[contains(.,'garage') or contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//tr[td[contains(.,'Balkon')]]/td[2]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        energy_label = response.xpath("//tr[td[contains(.,'Energieeffizienzklasse')]]/td[2]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        description = " ".join(response.xpath("//div[@class='box freitexte']/div//text()[normalize-space()]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [x for x in response.xpath("//div[@class='galerie']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Kircher Immobilien")
        item_loader.add_value("landlord_phone", "06222-8968")
        item_loader.add_value("landlord_email", "Info@Kircher-Immobilien.de")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("wohnung" in p_type_string.lower() or "appartement" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "haus" in p_type_string.lower()):
        return "house"
    else:
        return None

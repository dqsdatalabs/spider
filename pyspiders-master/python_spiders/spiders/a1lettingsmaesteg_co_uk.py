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

class MySpider(Spider):
    name = 'a1lettingsmaesteg_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        
        formdata = {
            "List_TRATYP": "2",
            "List_MINPRI": "XXX",
            "List_MAXPRI": "XXX",
            "List_BEDROO": "XXX",
            "store_tratyp": "2",
            "store_minpri": "",
            "store_maxpri": "",
            "store_locati": "",
            "store_bedroo": "",
            "store_protyp": "",
            "store_prosec": "",
            "store_perpag": "",
            "store_proage": "",
            "store_under_offer": "",
            "store_sold_stc": "",
            "store_reload": "",
            "button.x": "69",
            "button.y": "15",
        }

        yield FormRequest(
            url="http://a1lettingsmaesteg.co.uk/ipm/properties/search_list.php",
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'more information')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@class='prevnext']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
        
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[@class='flagimage_details']/img/@src").get()
        if status and "let.png" in status:
            return
        
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//h4[.='Description']//following-sibling::*/text()").getall())
        if desc and "commercial" in desc:
            return 
        elif desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and ("house" in desc.lower() or "mid terraced" in desc.lower() or "terrace" in desc.lower()):
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        item_loader.add_value("external_source", "A1lettingsmaesteg_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.url.split('ref=')[-1].split('&')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h3/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("title", address.strip())
            if '.' in address:
                city = address.split('.')[0].split(',')[-1].strip()
                zipcode = address.split('.')[-1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            elif "," in address:
                city = address.split(',')[-1].strip()
                if city.isalpha():
                    item_loader.add_value("city", city)

               

        description = " ".join(response.xpath("//h4[contains(.,'Description')]/following-sibling::p/text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
        room_count = response.xpath("//span[contains(.,'No bed')]/text()").get()
        if room_count:
            room_count = room_count.lower().split('no beds:')[-1].split('|')[0].strip()
            if room_count != '0':
                item_loader.add_value("room_count", room_count)
            else:
                if "studio" in desc.lower():
                    item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split('bathroom')[0].strip()
            if bathroom_count != '':
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                item_loader.add_value("bathroom_count", '1')
        
        rent = response.xpath("//span[contains(.,'pcm')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('£')[-1].split('pcm')[0].replace(',', '').strip())
            item_loader.add_value("currency", 'GBP')

        images = [response.urljoin(x) for x in response.xpath("//img[contains(@id,'img')]/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        label = "".join(response.xpath("//h4[contains(.,'EPC')]/following-sibling::table//tr//text()").extract())
        if label:
            e_label = "".join(label.strip().split(" ")[1:])
            energy_label = e_label.strip()[0:2]
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        
        pets_allowed = response.xpath("//li[contains(.,'NO PETS ALLOWED') or contains(.,'No pets')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)

        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li[contains(.,'Garage')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        deposit = response.xpath("//li[contains(.,'Rent in Advance Required')]//text()[contains(.,'£')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("£")[1].split(".")[0].strip())


        item_loader.add_value("landlord_phone", '01656 737773')
        item_loader.add_value("landlord_email", 'info@a1lettingsmaesteg.co.uk')
        item_loader.add_value("landlord_name", 'A1 Lettings')

        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
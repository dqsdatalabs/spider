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
import re
from datetime import datetime


class MySpider(Spider):
    name = 'crevits_be' 
    execution_type='testing'
    country='belgium'
    locale='nl' 
    external_source="Crevits_PySpider_belgium" 
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://crevits.be/nl/te-huur/appartementen/", "property_type": "apartment"},
	        {"url": "https://crevits.be/nl/te-huur/woningen/", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[contains(@class,'properties')]//div[@class='col-sm-12' and not(contains(.,'verhuurd'))]//div[@class='spotlight__content']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "normalize-space(//h1/text())")
        
        room_count = response.xpath(
            "//div/i[contains(@class,'bed')]/parent::div/following-sibling::div[contains(.,'slaapkamer')]/text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            if room_count != "0":
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div/i[contains(@class,'shower')]/parent::div/div[contains(.,'badkamer')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
         
        
        square_meters = response.xpath("//div/i[contains(@class,'home')]/parent::div/div[contains(.,'woon')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        else:
            square_meters = response.xpath("//td[contains(.,'opperv') or contains(.,'Opperv')]/following-sibling::td/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        rent = response.xpath("//td[contains(.,'Prijs')]/following-sibling::td/text()[contains(.,'€')]").get()
        if rent:
            price = rent.split("€")[1].split(",")[0].strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//div[contains(@class,'adress__street')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(",")[-1].strip().split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = "".join(response.xpath("//div[contains(@id,'Description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        latitude = response.xpath("//div/@data-geolat").get()
        longitude = response.xpath("//div/@data-geolong").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//div[@id='pand-carousel']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//td[contains(.,'Vrij')]/following-sibling::td/text()").get()
        if available_date:
            if "onmiddellijk" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed: 
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        date_check=item_loader.get_output_value("available_date")
        if not date_check:
            available_date=response.xpath("//td[.='Vrij op']/following-sibling::td/text()").get()
            if available_date:
                item_loader.add_value("available_date",available_date)
        
        floor = response.xpath("//td[contains(.,'Verdiep')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
            
        utilities = response.xpath("//td[contains(.,'kosten')]/following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        deposit = response.xpath("//td[contains(.,'Huur')]/following-sibling::td/text()").get()
        if deposit:
            deposit = deposit.split("€")[1].split(",")[0].strip().replace(".","")
            item_loader.add_value("deposit", int(float(deposit)))
            
        energy_label = response.xpath("//div/i[contains(@class,'leaf')]/parent::div/div[contains(.,'E')]/text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].split("kW")[0].strip().replace(",",".")
            item_loader.add_value("energy_label", energy_label_calculate(int(float(energy_label))))
        
        external_id = response.xpath("//div[contains(@class,'ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        furnished = response.xpath("//td[contains(.,'Badkamer type')]/following-sibling::td/text()[contains(.,'Ingericht')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//td[contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//td[contains(.,'Terras')]/text()").get()
        if terrace:
            if "balkon" in terrace.lower():
                item_loader.add_value("balcony", True)
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//td[contains(.,'Lift')]/following-sibling::td/text()").get()
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
                    
        item_loader.add_value("landlord_name", "IMMOBILIEN CREVITS")
        item_loader.add_value("landlord_phone", "32 9 222 27 76")
        item_loader.add_value("landlord_email", "immo@crevits.be")
        
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
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
 

class MySpider(Spider):
    name = 'huurinc_nl'
    start_urls = ['https://www.huurinc.nl/aanbod-panden/?min-price=&max-price=&city=&interior=&bedrooms=&available_at=']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Huurinchousing_PySpider_netherlands_nl'
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }
    #{'Appartement': 'OK', 'Hoekhuis': 'OK', 'Kamer': 'OK', None: 'OK', 'gezinswoning': 'OK', 
    # 'Benedenwoning': 'OK', 'Bovenwoning': 'OK', 'Penthouse': 'OK', 'Studio': 'OK', 'Stacaravan': 'OK', 
    # 'Inpandige garage': 'OK', 'Villa': 'OK'}

    # 1. FOLLOWING
    def parse(self, response):
    
        for item in response.xpath("//div[@class='aw-card']"):
            follow_url = item.xpath("./a/@href").get()
            square_meters = item.xpath(".//div[@class='aw-card-lower-body']/div[contains(.,'m²')]/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"square_meters":square_meters})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        page_404 = response.xpath("//title//text()[contains(.,'Pagina niet gevonden')]").get()
        if page_404:
            return 
        status = response.xpath("(//div[@class='aw-property-data-item']//p[contains(.,'Verhuurd')]//text())[2]").get()
        if status and "Verhuurd" in status:
            return
        description = "".join(response.xpath("//div[@class='property-label']/following-sibling::div//p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

            if "appartement" in description:
                item_loader.add_value("property_type", "apartment")
            elif "woning" in description:
                item_loader.add_value("property_type", "house")
            elif "kamer" in description.lower():
                item_loader.add_value("property_type", "house")
            elif "studio" in description.lower():
                item_loader.add_value("property_type", "house")
            else:
                return
            
            if 'gestoffeerd' in description.lower() or 'gemeubileerd' in description.lower():
                item_loader.add_value("furnished", True)

            if "parkeren" in description.lower() or "parkeerplaats" in description.lower():
                item_loader.add_value("parking", True)
            
            if "balkon" in description.lower():
                item_loader.add_value("balcony", True)
        
        item_loader.add_value("external_source", "Huurinchousing_PySpider_" + self.country + "_" + self.locale)
        
        address = response.xpath("//p[contains(.,'Adres')]//text()").getall()
        if address:
            item_loader.add_value('address', address[-1].strip())
            item_loader.add_value('city', address[-1].strip())
        zipcode = response.xpath("//p[contains(.,'Postcode')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        item_loader.add_value("title", 'Huurinc Housing')
        item_loader.add_value("external_link", response.url)

        external_id = response.url
        if external_id.endswith('/'):
            external_id = external_id.split('/')[-2].strip()
        else:
            external_id = external_id.split('/')[-1].strip()
        item_loader.add_value("external_id", external_id)

        room_count = response.xpath("(//p//strong[contains(.,'Slaapkamers')]/parent::p//text())[2]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif not room_count:
            if "studio" in description.lower() or "room" in description.lower():
                item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("(//p//strong[contains(.,'Badkamers')]/parent::p//text())[2]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        else:
            toiletten = response.xpath("//div[@class='aw-property-data-item']/p/strong[contains(.,'Toiletten')]/parent::p/text()").get()
            if toiletten:
                item_loader.add_value("bathroom_count", toiletten.strip())
        
        price = response.xpath("//p[contains(.,'Prijs')]/text()").re_first(r'\d+')
        if price:
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
       
        square_meters = response.meta.get("square_meters")
        if square_meters:
            item_loader.add_value("square_meters", re.sub(r"\D", "", square_meters))     
       
        if 'huisdieren zijn niet' in description.lower() or 'geen huisdieren' in description.lower():
            item_loader.add_value("pets_allowed", False)
        if 'wasmachine' in description.lower():
            item_loader.add_value("washing_machine", True)
        if 'zwembad' in description.lower():
            item_loader.add_value("swimming_pool", True)
        if 'terras' in description.lower():
            item_loader.add_value("terrace", True)
        
        images = [x for x in response.xpath("//ul[@id='lightSlider']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))        
        
        deposit = response.xpath("//p[contains(.,'Waarborgsom')]/text()").re_first(r"Waarborgsom: € (\d.*\d+)")
        if deposit:
            item_loader.add_value("deposit", deposit.replace(".", ""))
    
        # available_date = response.xpath("//text()[contains(.,'Beschikbaar per')]").get()
        # if available_date:
        #     available_date = available_date.split('per ')[-1].strip()
        #     date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"], languages=['nl'])
        #     if date_parsed:
        #         date2 = date_parsed.strftime("%Y-%m-%d")
        #         item_loader.add_value("available_date", date2)
        # else:
        #     available_date = response.xpath("//p[./span[.='Beschikbaar']]/span[2]/text()").get()
        #     if available_date and available_date.replace(" ","").replace("-","").replace("/","").isalpha() != True:
        #         try:
        #             available_date = available_date.split(" ")[1].strip()
        #         except: if "parkeren" in desc:
        #         item_loader.add_value("parking", True)
        #         date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
        #         date2 = date_parsed.strftime("%Y-%m-%d")
        #         item_loader.add_value("available_date", date2)

        item_loader.add_value('landlord_name', "Huurinc Housing")
        item_loader.add_value('landlord_email', "info@huurinc.nl")
        item_loader.add_value('landlord_phone', "+31 (0) 40-7515151")

        yield item_loader.load_item()
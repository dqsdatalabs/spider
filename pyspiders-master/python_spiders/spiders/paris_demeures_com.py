# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'paris_demeures_com_disabled'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.paris-demeures.com/properties/?filter_contract_type=7&filter_location=&filter_bedrooms=&filter_price_from=&filter_price_to=&filter_area_from=",
            },
            {
                "url": "http://www.paris-demeures.com/properties/?filter_contract_type=5&filter_location=&filter_bedrooms=&filter_price_from=&filter_price_to=&filter_area_from="
            }
        ]
        for item in start_urls:
            yield Request(
                item.get('url'),
                callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='title']//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//th[contains(.,'Type bien')]/following-sibling::td/text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: print(response.url)
        item_loader.add_value("external_source", "Paris_Demeures_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//th[contains(.,'Ville')]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        square_meters = response.xpath("//th[contains(.,'Surface')]/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//th[contains(.,'Chambre')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//th[contains(.,'Salle')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//th[contains(.,'Loyer')]/following-sibling::td/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//th[contains(.,'Référence')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        terrace = response.xpath("//li[@class='checked']/text()[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[@class='checked']/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        description = " ".join(response.xpath("//div[@class='property-detail']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        else:
            description = " ".join(response.xpath("//div[@class='property-detail']/div/p/..//text()").getall())
            if description:
                item_loader.add_value("description", description.strip())
        
        import dateparser
        if "DISPONIBLE LE" in description:
            available_date = description.split("DISPONIBLE LE")[1].split("–")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@class='content']//li//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Paris Demeures")
        item_loader.add_value("landlord_phone", "33 (0)1 56 43 41 41")
        item_loader.add_value("landlord_email", "paris@paris-demeures.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None
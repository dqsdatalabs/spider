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
    name = 'agencebfm_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencebfm_Immo_PySpider_france'
    custom_settings = {              
        "PROXY_ON" : True,
        "CONCURRENT_REQUESTS": 3,        
        "COOKIES_ENABLED": False,        
        "RETRY_TIMES": 3,        

    }
    download_timeout = 120
    def start_requests(self):
        yield Request("https://www.agencebfm-immo.com/fr/locations/1", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'vignette-mf')]/div[contains(@class,'card')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='card-image']/a/@href").get())
            property_type = item.xpath(".//h3/text()[1]").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//i[contains(.,'chevron_right')]/../@data-href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Agencebfm_Immo_PySpider_france")
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        rent = "".join(response.xpath("//div[@class='charges']/span/text()").getall())
        if rent:
            price = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//span[@class='commune']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        zipcode = response.xpath("//h1//text()").get()
        if zipcode:
            if "-" in zipcode:
                zipcode_ = zipcode.split("-")[-2].strip().split(" ")[-1]
                if zipcode_.isdigit():
                    item_loader.add_value("zipcode", zipcode_)
                else:
                    zipcode_ = zipcode.split("à")[-1].strip().split(" ")[1]
                    if zipcode_:
                        item_loader.add_value("zipcode",zipcode_)
        
        square_meters = response.xpath("//span[contains(.,'habitable')]/following-sibling::b/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//span[contains(.,'Chambre')]/following-sibling::b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'pièces')]/following-sibling::b/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[contains(.,'Salle')]/following-sibling::b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::b/text()").get()
        if deposit:
            deposit = deposit.split(" ")[0]
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[contains(.,'Charge')]/following-sibling::b/text()").get()
        if utilities:
            utilities = utilities.split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        parking = response.xpath("//span[contains(.,'Garage') or contains(.,'Parking')]/following-sibling::b/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::b/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::b/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::b/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//span[contains(.,'Disponibilité')]/following-sibling::b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//span[contains(.,'Étage')]/following-sibling::b/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        description = " ".join(response.xpath("//div[@id='description']//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[contains(@class,'owl-carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'position = [')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('position = [')[1].split(',')[0]
            longitude = latitude_longitude.split('position = [')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        energy_label = response.xpath("//div[@class='valeur_conso']/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[0])
        
        item_loader.add_value("landlord_name", "Agence immobilière Annecy - Agence BFM")
        item_loader.add_value("landlord_phone", "04 50 51 12 16")
        
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
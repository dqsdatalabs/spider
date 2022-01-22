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
import dateparser

class MySpider(Spider):
    name = 'alpimmo_be'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    custom_settings = {
        "PROXY_ON": True,
    }


    # 1. FOLLOWING
    def start_requests(self):
        url = "https://www.alpimmo.be/a-louer.php"
        yield Request(url,callback=self.parse)

    def parse(self, response):
        for item in response.xpath("//div[@class='listing']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

        page = response.xpath("//div[contains(@class,'bloc_texte pagination')]/a[.='>']/@href").extract_first()
        if page:
            next_page = response.urljoin(page)
            yield Request(next_page,callback=self.parse)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Alpimmo_PySpider_belgium")
        item_loader.add_value("external_link", response.url)

        property_type = "".join(response.xpath("//h1[@class='titre_main']/text()").getall())
        if get_p_type_string(property_type):
            
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
           
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)

        if "Vente" in title:
            return

        address = response.xpath("//div[@class='top_page animate fade_in']/p/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.strip().split(" ")[0].strip())
            item_loader.add_value("city", address.strip().split(" ")[1].strip())

        item_loader.add_xpath("external_id", "//span[@class='ref']/i/text()")
        
        rent = response.xpath("//h1[@class='titre_main']/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        room_count = response.xpath("//div[@class='grid_carac']//div[@class='item'][contains(.,'chambres')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//li[@class='libele'][contains(.,'habitable')]/following-sibling::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        else:
            square_meters = response.xpath("//div[@class='item']/span[contains(.,'m')]/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.replace("m²","").strip())

        
        desc = " ".join(response.xpath("//div[contains(@class,'bloc_texte')]/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle de bain" in desc:
            bathroom_count = desc.split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("au","").replace("e","").replace("\u00e8m","")
            if floor.replace("(","").isdigit():
                item_loader.add_value("floor", floor.replace("(",""))
        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'item')]/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "Garantie locative:" in desc:
            deposit = desc.split("Garantie locative:")[1].strip().split(" ")[0]
            if deposit.isdigit():
                price = rent.replace("€","")
                deposit = int(deposit)*int(price)
                item_loader.add_value("deposit", deposit)
        
        if "Charges communes:" in desc:
            utilities = desc.split("Charges communes:")[1].strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)
        elif "de charges" in desc:
            utilities = desc.replace("/mois de provisions","").split("de charges")[0].strip().split(" ")[-1].replace("\u20ac","")
            item_loader.add_value("utilities", utilities)
        
        external_id = response.xpath("//h1[@class='titre']/b/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].split(")")[0].strip())
        
        energy_label = response.xpath("//div[@class='peb']/div/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        parking = response.xpath("//li[@class='libele'][contains(.,'parking')]/following-sibling::li/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@class='item'][contains(.,'parking')]/span/text()[.!='0']").get()
            if parking:
                item_loader.add_value("parking", True)

        
        match = re.search(r'(\d+/\d+/\d+)', desc)
        available_date = ""
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        elif "Libre au" in desc:
            available_date = desc.split("Libre au")[1].split(".")[0].strip()
        elif "Disponible au" in desc:
            available_date = desc.split("Disponible au")[1].split("\n")[0].strip()
        
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "ALPIMMO AGENCE IMMOBILIERE")
        item_loader.add_value("landlord_phone", "065 / 34 84 24")
        item_loader.add_value("landlord_email", "secretariat@alpimmo.be")
        
                
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "appartement" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
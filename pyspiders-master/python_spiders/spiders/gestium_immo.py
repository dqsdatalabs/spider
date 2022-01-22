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
    name = 'gestium_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://gestium.immo/properties-search-results/?search_lat=&search_lng=&sort=newest&search_city=&search_min_price=&search_max_price=&search_category=2&search_type=3&search_min_area=&search_max_area=&visite_virtuelle=&visite_virtuelle_comparison=equal"]
    external_source="Gestium_PySpider_france"
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='card']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h1/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='entry-content']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        item_loader.add_value("external_source", self.external_source)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        
        title = response.xpath("//h1[@class='pageTitle']/text()").get()
        if title:
            item_loader.add_value("title", title)
            if "meublé" in title.lower() or "meuble" in title.lower():
                item_loader.add_value("furnished", True)

        
        address = "".join(response.xpath("//div[@class='address']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//div[@class='address']/text()[2]").get()
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip())
            item_loader.add_value("zipcode", city.split(",")[0].strip())
        citycheck=item_loader.get_output_value("city")
        if not citycheck:
            city1=response.xpath("//title/text()").get()
            if city1:
                item_loader.add_value("city",city1.split("-")[-1].strip())

        rent = response.xpath("//div[@class='listPrice']/text()[contains(.,'€')]").get()
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        utility=" ".join(response.xpath("//div[@class='entry-content']//p//text()").getall())
        if utility:
            utilityindex=utility.find("Loyer")
            utility=utility[utilityindex:]
            if utility and "+" in utility:
                utility=utility.split("+")[-1].split("€")[0].strip()
                if utility:
                    item_loader.add_value("utilities",utility)

        
        square_meters = response.xpath("//strong[.='Surface']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].strip())
        
        room_count = response.xpath("//strong[contains(.,'chambre')]/following-sibling::text()[not(contains(.,'0'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        else:
            room_count = response.xpath("//strong[contains(.,'pièce')]/following-sibling::text()[not(contains(.,'0'))]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        bathroom_count = response.xpath("//strong[contains(.,'salle')]/following-sibling::text()").get()
        if bathroom_count and not "0" in bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        bathcheck=item_loader.get_output_value("bathroom_count")
        if not bathcheck:
            bath=response.xpath("//ul[@class='features']//li//div[contains(.,'Salle')]/text()").get()
            if bath:
                bath=re.findall("\d+",bath)
                item_loader.add_value("bathroom_count",bath)
        
        parking = response.xpath("//strong[contains(.,'stationnement')]/following-sibling::text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit = response.xpath("//div[@class='entry-content']//p//text()[contains(.,'Dépôt ')]").get()
        if deposit:
            deposit = deposit.replace(":","=").split("=")[1].strip().split(" ")[0]
            if "€" in deposit: deposit = deposit.split("€")[0]
            else: deposit = int(deposit)*int(float(rent))
            item_loader.add_value("deposit", deposit)
        
        description = " ".join(response.xpath("//div[@class='entry-content']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//span[@class='diagnostic-number']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        available_date = ""
        if "à compter du" in description:
            available_date = description.split("à compter du")[1].split(". ")[0].strip()
        elif "Libre mi" in description:
            available_date = description.split("Libre mi")[1].split("Loyer")[0].strip()
        
        import dateparser
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                date_parsed = dateparser.parse(available_date.split(" ")[0], date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        furnished = response.xpath("//h1/text()[contains(.,'Meublé')]").get()
        if furnished: item_loader.add_value("furnished", True)
            
        item_loader.add_xpath("landlord_name", "//div[@class='agentName']/text()")
        item_loader.add_value("landlord_phone", "06 84 50 98 58")
        email=response.xpath("//input[@id='agent_email']/@value").get()
        if email:
            item_loader.add_value("landlord_email",email)
        
        yield item_loader.load_item()
            

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None
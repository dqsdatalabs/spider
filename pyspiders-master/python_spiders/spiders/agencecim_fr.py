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
    name = 'agencecim_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.agencecim.fr/a-louer/departement-31/1"},
            {"url": "https://www.agencecim.fr/a-louer/departement-82/1"},
            {"url": "https://www.agencecim.fr/a-louer/departements-30-34-11-66/1"}
            ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//li/@onclick").extract():
            follow_url = response.urljoin(item.split("location.href='")[1].split("'")[0])
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = response.url.split(f"/{page}")[0] + f"/{page+1}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Agencecim_PySpider_france")
        
        title = response.xpath("//div[@class='bienTitle']/h2//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title)
            item_loader.add_value("title", title)
        prop_type = ""
        if get_p_type_string(title):
            prop_type = get_p_type_string(title)
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        
        item_loader.add_value("external_link", response.url)
        
        address = response.xpath("//section/div[contains(@class,'elementDtTitle')]/h1/text()").get()
        if address:
            item_loader.add_value("address", address)
        
        city = response.xpath("//p/span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode = response.xpath("//p/span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        square_meters = response.xpath("//p/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        rent = response.xpath("//p/span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(" ","").replace(",",".")
            item_loader.add_value("rent", int(float(price)))
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p/span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
                
        bathroom_count = response.xpath("//p/span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        match = re.search(r'(\d+/\d+/\d+)', desc)
        available_date = ""
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        elif "disponible au" in desc.lower():
            available_date = desc.lower().split("disponible au")[1].split("\n")[0].replace(".","").strip()
        elif "disponible le" in desc.lower():
            available_date = desc.lower().split("disponible le")[1].split("\n")[0].replace(".","").strip()
        elif "Disponible" in desc:
            available_date = desc.split("Disponible")[1].replace("\u00e0 partir du","").replace("\u00e0 compter du","").strip()
            if "\n" in available_date:
                available_date = available_date.split("\n")[0].strip()
        
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
           
        floor = response.xpath("//p/span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        deposit = response.xpath("//p/span[contains(.,'de garantie')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities = response.xpath("//p/span[contains(.,'Charges')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[1].strip())
        
        elevator = response.xpath("//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//p/span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//p/span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        
        parking = response.xpath("//p/span[contains(.,'garage')]/following-sibling::span/text()[.!='0'] | //span[contains(.,'Nombre de parking')]/following-sibling::span[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True) 
        
        lat_lng = response.xpath("//script[contains(.,'lat')]/text()").get() 
        if lat_lng:
            latitude = lat_lng.split("Map.setCenter")[-1].split(",")[1].split("lat:")[-1].strip()
            longitude = lat_lng.split("Map.setCenter")[-1].split(");")[0].split("lng:")[-1].split("}")[0].strip()
            if not "type" in latitude:
                item_loader.add_value("longitude", longitude)
            if not "type" in longitude:
                item_loader.add_value("latitude", latitude)
            
        item_loader.add_value("landlord_name", "CONCEPT IMMOBILIER DU MIDI")
        item_loader.add_value("landlord_phone", "05 63 65 18 40")
        item_loader.add_value("landlord_email", "immo@agencesudimmo.fr")
          
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "apartment" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "détachée" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None
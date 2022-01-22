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
    name = 'semcoda_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        formdata = {
            "type_offre": "",
            "localisationr": "",
            "prixminr": "0",
            "prixmaxr": "5000",
            "surfaceminr": "0",
            "surfacemaxr": "500",
            "form_build_id": "form-mGpvMx6yieYS_R_P3cQ1GiUOYbDv5IRDURInJ1s5SUA",
            "form_id": "search_form_rent",
            "op": "Rechercher",
        }
        yield FormRequest("https://www.semcoda.com/offre/search/map/2",
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True)

    # 1. FOLLOWING
    def parse(self, response):
        script_data = response.xpath("//script[@type='application/json']//text()").get()
        if script_data:
            data = json.loads(script_data)
            for item in data["offre"]["items"]:
                lat, lng = item["localisation"]["latitude"], item["localisation"]["longitude"]
                html_link = item["bulle"]
                sel = Selector(text=html_link, type="html")
                f_url = response.urljoin(sel.xpath("//a/@href").get())
                yield Request(
                    f_url,
                    callback=self.populate_item,
                    meta={
                        "lat":lat,
                        "lng":lng,
                    }
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("latitude", response.meta["lat"])
        item_loader.add_value("longitude", response.meta["lng"])
        f_text = "".join(response.xpath("//h1/span/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        item_loader.add_value("external_source", "Semcoda_PySpider_france")
        
        title = response.xpath("//h1/span/text()").get()
        item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//p[@class='adresse']//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
            
        city = response.xpath("//p[@class='adresse']/strong/text()").get()
        if city:
            if city.split(" ")[0].isdigit():
                item_loader.add_value("zipcode", city.split(" ")[0])
            if not city.split(" ")[1].isdigit():
                item_loader.add_value("city", city.split(" ")[1])
        
        room_count = response.xpath("substring-after(//div[@class='etage-piece']/span/text()[contains(.,'pièce')],':')").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        rent = response.xpath("//td/span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//td[contains(.,'Superficie')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        floor = response.xpath("substring-after(//div[@class='etage-piece']/span/text()[contains(.,'Étage')],':')").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        external_id = response.xpath("//td[contains(.,'Réf')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        import dateparser
        available_date = response.xpath("//div[contains(@class,'description')]/p//text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split(".")[0].split(" ")[-1]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        energy_label = response.xpath("substring-after(//div[@class='diagnostic']/span[contains(.,'DPE')]/text(),':')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'description')]/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'lon=')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat=')[1].split('&')[0]
            longitude = latitude_longitude.split('lon=')[1]     
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//div[@class='ls-slide']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "SEMCODA")
        item_loader.add_value("landlord_phone", "04 78 93 21 92")
        
        yield item_loader.load_item()
            

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "duplex" in p_type_string.lower() or "flat" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None
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
    name = 'valexim_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "PROXY_ON": True    
    }
    formdata = {
        'location_search[typeBien][]': '',
        'location_search[tri]': 'loyerCcTtcMensuel|asc',
    }
    index=0
    def start_requests(self):
        self.type_list = [
            {
                "property_type": "apartment",
                "type":"1"
            },
            {
                "property_type": "house",
                "type": "2"
            }
        ]
        # for item in type_list:
        self.formdata["location_search[typeBien][]"] = self.type_list[self.index].get("type")
        yield FormRequest("https://www.valexim.fr/fr/locations", 
                        dont_filter=True, 
                        formdata=self.formdata,
                        callback=self.parse, 
                        meta={"property_type": self.type_list[self.index].get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 20)
        seen = False

        for item in response.xpath("//div[@class='informations_bien']/h3/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 20 or seen:
            headers = {
                'authority': 'www.valexim.fr',
                'accept': 'text/html, */*; q=0.01',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36 Edg/88.0.705.81',
                'x-requested-with': 'XMLHttpRequest',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer': 'https://www.valexim.fr/fr/locations',
                'accept-language': 'tr,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            }
            follow_url = f"https://www.valexim.fr/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc"
            yield Request(follow_url, dont_filter=True, headers=headers, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 20})
        else:
            self.index +=1
            if self.index < len(self.type_list): 
                self.formdata["location_search[typeBien][]"] = self.type_list[self.index].get("type")
                yield FormRequest("https://www.valexim.fr/fr/locations", 
                        dont_filter=True, 
                        formdata=self.formdata,
                        callback=self.parse, 
                        meta={"property_type": self.type_list[self.index].get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])  
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Valexim_PySpider_france")

        external_id = response.xpath("//span[contains(@class,'reference')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'bandeauFooterLocation')]//span[contains(@class,'commune')]//text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//div[contains(@class,'bandeauFooterLocation')]//span[contains(@class,'commune')]//text()").get()
        if city:
            item_loader.add_value("city", city)

        zipcode = response.xpath("//div[contains(@class,'bandeauFooterLocation')]//span[contains(@class,'cp')]//text()").get()
        if zipcode:
            zipcode = zipcode.replace("(","").replace(")","")
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//span[contains(.,'Surface')]//following-sibling::b//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(".")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//div[contains(@class,'rowPrix')]//span/text()").get()
        if rent:
            rent = rent.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'charges')]//div[contains(.,'garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//div[contains(@class,'charges')]//text()[contains(.,'provision pour charges')]").get()
        if utilities:
            utilities = utilities.split("€")[0].replace("-","").strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@id,'desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(.,'pièces')]//following-sibling::b//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'pièce')]//following-sibling::b//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(.,'Salle')]//following-sibling::b//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'carousel-photo')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//p[contains(.,'Disponible')]//following-sibling::b//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        balcony = response.xpath("//span[contains(.,'balcon')]//following-sibling::b//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]//following-sibling::b//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]//following-sibling::b//text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//span[contains(.,'Parking') or contains(.,'Garage')]//following-sibling::b//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]//following-sibling::b//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//span[contains(.,'Étage')]//following-sibling::b//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//span[contains(.,'Consommation énergétique')]//following-sibling::div//span[contains(.,'*')]//text()").get()
        if energy_label:
            energy_label = energy_label.replace("*","").split(".")[0]
            item_loader.add_value("energy_label", energy_label)

        landlord_name = "".join(response.xpath("//div[contains(@class,'detail_nego')]//h3/text()").getall())
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name.strip())
            if "LA COTE SAINT ANDRÉ" in landlord_name:
                item_loader.add_value("landlord_phone", "04 74 20 23 41")
            elif "GRENOBLE GAMBETTA" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 12 17 80")
            elif "VOIRON" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 05 51 83")
            elif "CROLLES" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 08 84 66")
            elif "BOURGOIN-JALLIEU" in landlord_name:
                item_loader.add_value("landlord_phone", "04 74 43 58 58")
            elif "LA TOUR DU PIN" in landlord_name:
                item_loader.add_value("landlord_phone", "04 74 43 58 68")
            elif "LA VERPILLIÈRE" in landlord_name:
                item_loader.add_value("landlord_phone", "04 74 43 58 58")
            elif "COUBLEVIE" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 67 52 74")
            elif "SAINT ÉGRÈVE" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 94 09 40")
            elif "GRENOBLE MALLIFAUD" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 47 15 88")
            elif "GRENOBLE Entreprises & Commerces" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 18 59 20")
            elif "LE TOUVET" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 08 58 76")
            elif "ALLEVARD" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 04 98 02")
            elif "PONTCHARRA" in landlord_name:
                item_loader.add_value("landlord_phone", "04 76 97 94 00")
            elif "ALBERTVILLE" in landlord_name:
                item_loader.add_value("landlord_phone", "04 79 32 06 20")
            elif "ANNECY" in landlord_name:
                item_loader.add_value("landlord_phone", "04 50 33 44 44")
            else:
                item_loader.add_value("landlord_phone", "04 50 92 18 30")
            

        item_loader.add_value("landlord_email", "contact@valexim.fr")
        script_map = response.xpath("//script[contains(.,'position = [')]/text()").get()
        if script_map:
            latlng = script_map.split("position = [")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        yield item_loader.load_item()
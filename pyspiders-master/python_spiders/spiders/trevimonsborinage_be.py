# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = "trevimonsborinage_be"
    start_urls = ["https://www.trevimonsborinage.be/"]  # LEVEL 1
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    
    # 1. FOLLOWING
    def parse(self, response):
        self.url = "https://www.trevimonsborinage.be/fr/residentiel/louer-bien-immobilier"

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.trevimonsborinage.be"
        }
        self.data = {
            "property-type": "1",
            "property-address": "",
            "property-prix--min": "",
            "property-prix--max": "",
            "property-chambre": "",
            "search": "1",
            "check": "",
            "target": "3",
        }

        yield FormRequest(
            self.url,
            headers=self.headers,
            formdata=self.data,
            dont_filter=True,
            callback=self.jump_first,
        )

    def jump_first(self, response):
        self.start_urls = [
            {"url": "https://www.trevimonsborinage.be/fr/residentiel/louer-bien-immobilier/maison", "property_type": "house", "type":"1"},
            {"url": "https://www.trevimonsborinage.be/fr/residentiel/louer-bien-immobilier/flat", "property_type": "apartment", "type":"34"},
            {"url": "https://www.trevimonsborinage.be/fr/residentiel/louer-bien-immobilier/appartement", "property_type": "apartment", "type":"2"},
            {"url": "https://www.trevi.be/fr/residentiel/louer-bien-immobilier/maison", "property_type": "house", "type":"1"},
            {"url": "https://www.trevi.be/fr/residentiel/louer-bien-immobilier/flat", "property_type": "apartment", "type":"34"},
            {"url": "https://www.trevi.be/fr/residentiel/louer-bien-immobilier/appartement", "property_type": "apartment", "type":"2"}
        ]  # LEVEL 1
        self.index = response.meta.get("index",0)
        
        yield Request(url=self.start_urls[self.index].get('url'),
                        callback=self.jump,
                        dont_filter = True,
                        meta={'property_type': self.start_urls[self.index].get('property_type'),
                        "type":self.start_urls[self.index].get('type')})

    def jump(self, response):

        seen = False
        for card in response.xpath('//a[@class="card bien"]/@href').extract():
            url = response.urljoin(card)
            if "projets" not in url:
                yield response.follow(url, self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        headers = {
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://www.trevimonsborinage.be",
        }
        page = response.meta.get("page", 1)
        data = {
            "limit1": "12",
            "limit2": str(page * 12),
            "serie": str(page),
            "filtre": "filtre_cp",
            "market": "",
            "lang": "fr",
            "type": f"{response.meta.get('type')}",
            "goal": "1",
            "property-type":f"{response.meta.get('type')}",
            "goal": "1",
            "search": "1",
        }
        if seen:
            yield FormRequest(
                "https://www.trevimonsborinage.be/Connections/request/xhr/infinite_projects.php",
                headers=headers,
                formdata=data,
                callback=self.jump,
                meta={"page": page + 1, 
                "type": response.meta.get('type'),
                "property_type": response.meta.get('property_type')},
            )
        else:
            if self.index < len(self.start_urls) - 1:
                self.data["property-type"] = self.start_urls[self.index + 1].get('type')
                yield FormRequest(
                    self.url,
                    headers=self.headers,
                    formdata=self.data,
                    callback=self.jump_first,
                    meta={"index":self.index+1}
                )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "normalize-space(//h1/text())")
        item_loader.add_value("external_source", "Trevimonsborinage_PySpider_belgium")

        item_loader.add_xpath("title", "//div[contains(@class,'content-intro')]/h2")
        room = "".join(response.xpath("//tr[contains(.,'Nbre de chambres')]/td[2]/text()[ .!='0']").extract())
        s_meter = response.xpath("//tr[contains(.,'Superficie habitable')or contains( .,'Superficie' )]/td[2]/text()[ . !='0 m²']").extract_first()
        prop_type = response.meta.get('property_type')
        property_type = response.xpath("//tr[contains(.,'Type de bien')]/td[2]//text()").extract_first()
        if property_type:
            if "studio" in property_type.lower():
                prop_type = "studio"
            elif "appartement" in property_type.lower():
                prop_type = "apartment"
        if prop_type:
            item_loader.add_value("property_type",prop_type)
        else: return
        
        item_loader.add_xpath("external_id", "//p[contains(.,'Référence :')]/b/text()")
        bathroom_count = "".join(response.xpath("//tr[contains(.,'bain')]/td[2]/text()[ .!='0']").extract())
        item_loader.add_value("bathroom_count", bathroom_count)
      
        description ="".join(response.xpath(
            "//div[@class='bien__content']//p[2]/text() | //div[@class='d-print-none']/p//text()"
        ).extract())
        if description:
            item_loader.add_value("description", description.strip())
            if "piscine" in description.lower():
                item_loader.add_value("swimming_pool",True)
            if "machine à laver" in description.lower():
                item_loader.add_value("washing_machine",True)
            if "terrasses" in description:
                item_loader.add_value("terrace", True)
            if "sans ascenseur" in description.lower():
                item_loader.add_value("elevator", False)
            if "Libre le" in description:
                try:
                    date_value = description.split("Libre le")[1].split(".")[0].strip()
                    item_loader.add_value("available_date",dateparser.parse(date_value).strftime("%Y-%m-%d"))
                except:
                    pass
        label = response.xpath("substring-before(substring-after(//div[@class='col-12']/img/@src,'_'),'.')").extract_first()
        if label:
            item_loader.add_value("energy_label",label.upper())
        else:
            energy_label = response.xpath("//tr[td/h3[contains(.,'E spec')]]/td[2]/text()").extract_first()
            if energy_label:
                energy =  energy_label.strip().split(" ")[0].strip()
                if energy.isdigit():
                    item_loader.add_value("energy_label",energy)    
        price = response.xpath(
            "//tr[contains(.,'Loyer / mois')]/td[2]/text()[contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[0])
            item_loader.add_value("currency", "EUR")

        utilities = response.xpath(
            "//tr[contains(.,'Charges / mois')]/td[2]/text()[contains(., '€')]"
        ).extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        address = "".join(
            response.xpath("//tr[contains(.,'Adress')]/td[2]/text()").extract()
        )
        if address and address.strip()!=',':
            
            item_loader.add_value("address", address.replace("  ",""))
            zip_code = response.xpath(
                "//tr[contains(.,'Code postal')]/td[2]/text()"
            ).extract_first()
            if zip_code:
                item_loader.add_value("zipcode", zip_code.split(" -")[0].strip())
                item_loader.add_value("city", zip_code.split(" -")[1].strip())
        else:
            address2 = response.xpath(
                "//tr[contains(.,'Code postal')]/td[2]/text()"
            ).extract_first()
            if address2:
                item_loader.add_value("address", address2)
                item_loader.add_value("zipcode", address2.split(" -")[0].strip())
                item_loader.add_value("city", address2.split(" -")[1].strip())
            
        if room:            
            if room != "0":    
                item_loader.add_value("room_count", room)
        elif not room:
            if description:
                if "studio" in property_type or "studio" in description.lower():
                    item_loader.add_value("room_count", "1")
                elif "chambre" in property_type:
                    item_loader.add_value("room_count", "1")
            
        if s_meter is not None:
            item_loader.add_value("square_meters", s_meter.replace("m²", ""))

        floor = response.xpath("//tr[td/h3[contains(.,'Etage')]]/td[2]/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor)
        
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='slider slider-bien--details h-100']//div/a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        parking = response.xpath(
            "//tr[contains(.,'Parking(s)')]/td[2]/text()[.!='0']"
        ).get()
        garage = response.xpath(
            "//tr[contains(.,'Garage(s)')]/td[2]/text()[.!='0']"
        ).get()
        if parking or garage:
            item_loader.add_value("parking", True)
            
        terrace = response.xpath("//div[@class='d-print-none']/p//text()[contains(.,'terrasse ')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//tr[contains(.,'Meublé')]/td[2]/text()").get()
        if furnished:
            if furnished == "Yes" or furnished == "Oui":
                item_loader.add_value("furnished", True)
        elif not furnished:
            if "meublé " in description:
                item_loader.add_value("furnished", True)



        item_loader.add_value("landlord_phone", "+32 65 22 01 23")
        item_loader.add_value("landlord_email", "info@trevimonsborinage.be")
        item_loader.add_value("landlord_name", "Trevimons Borinage")

        yield item_loader.load_item()

        yield item_loader.load_item()
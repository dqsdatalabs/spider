# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'ileverte_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.ileverte-immobilier.fr/locations-immobilieres/louer-logement_0_01000-00001-0-0-0-0-0_1-1.html", 
                "property_type": "studio"
            },
            {
                "url": "http://www.ileverte-immobilier.fr/locations-immobilieres/louer-logement_0_01000-00000-0-0-0-0-0_1-1.html", 
                "property_type": "apartment"
            },
	        {
                "url": "http://www.ileverte-immobilier.fr/locations-immobilieres/louer-logement_0_10000-00000-0-0-0-0-0_1-1.html", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[@class='ligneAnnonce ']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:
            f_url = response.url.replace(f"-{page-1}.html", f"-{page}.html")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("_")[-1].split(".")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ileverte_Immobilier_PySpider_france")

        title = " ".join(response.xpath("//div[contains(@class,'divInfosTop')]//div[contains(@class,'ligne3')]/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//div[contains(@class,'divInfosTop')]//div[contains(@class,'ligne3')]/text()").getall())
        if address:            
            address = re.sub('\s{2,}', ' ', address.replace(" -","").strip())
            if "La tronche" in address:
                city = "La tronche"
                zipcode = address.split(city)[0].strip().split(" ")[-1]
            else:
                city = address.split(" ")[-1]
                zipcode = address.split(" ")[-2]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        square_meters = response.xpath("//div[contains(@class,'ligne4')]//div[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//div[contains(@class,'infosSection')]//text()[contains(.,'Loyer')]").get()
        if rent:
            rent = rent.split(":")[1].split(",")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent.strip())
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'infosSection')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].replace("€","").strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//div[contains(@class,'infosSection')]//text()[contains(.,'Charge')]").get()
        if utilities:
            utilities = utilities.split(":")[1].replace("€","").split(",")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'divDescriptif')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'divInformationsGenerales')]//text()[contains(.,'Chambre')]").get()
        if room_count:
            room_count = room_count.replace("-","").strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        

        bathroom_count = response.xpath("//div[contains(@class,'divInformationsGenerales')]//text()[contains(.,'salle')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.replace("-","").strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@id,'conteneurVignettesGalerie')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'infosDispos')]//text()").getall())
        if available_date:
            if not "immédiat" in available_date.lower():
                available_date = available_date.split(":")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        terrace = response.xpath("//div[contains(@class,'divInformationsGenerales')]//text()[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//div[contains(@class,'divCarac')]//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//div[contains(@class,'divInformationsGenerales')]//text()[contains(.,'étage')]").get()
        if floor:
            floor = floor.replace("-","").strip().split(" ")[0]
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//div[contains(@class,'divConsoEmission')]//text()[contains(.,'Catégorie')]").get()
        if energy_label:
            energy_label = energy_label.split("Catégorie")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "Ile Verte Immobilier")
        item_loader.add_value("landlord_phone", "04 76 51 50 50")
        item_loader.add_value("landlord_email", "grenoble@ileverte.fr")
        
        yield item_loader.load_item()
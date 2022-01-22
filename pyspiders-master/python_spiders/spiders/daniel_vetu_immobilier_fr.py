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
    name = 'daniel_vetu_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.daniel-vetu-immobilier.fr/location/appartement/budget-min-0/budget-max-0/secteur-tous/surface-min-0-m2/surface-max-0-m2/p-t/stationnement-tous-0/options-aucune/chauffage-tous-modeles/proximites-ecoles-na--commerces-na--transports-na/page/1/", 
                "property_type": "apartment"
            },
	        {
                "url": "http://www.daniel-vetu-immobilier.fr/location/maison/budget-min-0/budget-max-0/secteur-tous/surface-min-0-m2/surface-max-0-m2/p-t/stationnement-tous-0/options-aucune/chauffage-tous-modeles/proximites-ecoles-na--commerces-na--transports-na/", 
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
        for item in response.xpath("//section[contains(@class,'trois_colonne')]//a[contains(@class,'annonce')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:
            f_url = response.url.replace(f"-na/page/{page-1}/", "-na/").replace("transports-na/",f"transports-na/page/{page}/")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Daniel_Vetu_Immobilier_PySpider_france")
        item_loader.add_xpath("external_id", "substring-after(//small[contains(.,'Réf. ')]//text(),'Réf. ')")
        item_loader.add_xpath("title", "//h1/strong/text()")

        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        city = response.xpath("//h1/strong/text()").get().split(" - ")[-1]
        if city and city.strip():
            item_loader.add_value("city", city.strip())
       
        rent = response.xpath("//span/strong[@class='txt_bleu']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        square_meters = response.xpath("//tr[td[.='Surface habitable']]/td[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.replace("/","").split("m")[0].strip()))))
        floor = response.xpath("//tr[td[.=\"Numéro de l'étage\"]]/td[2]/text()[1]").get()
        if floor:
            item_loader.add_value("floor", floor.replace("étage",""))
        room_count = (response.url).split("/")[-2].split("-")[0].strip("t")
        if room_count:
            item_loader.add_value("room_count", room_count.split("ch")[0])
        bathroom_count = response.xpath("//tr[td[.='Nombre de salles de bain' or .='Nombre de salles de douche']]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle")[0])
        utilities = response.xpath("//tr[td[.='Charges provisionnelles']]/td[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        deposit = response.xpath("//tr[td[.='Dépôt de garantie']]/td[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        desc = " ".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@itemprop='image']/meta[@itemprop='url']/@content").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = "".join(response.xpath("//tr[td[.='Stationnement']]/td[2]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)
        elevator = "".join(response.xpath("//tr[td[.='Divers']]/td[2]/text()[contains(.,'Ascenseur')]").getall())
        if elevator:
            item_loader.add_value("elevator", True)
        terrace = "".join(response.xpath("//tr[td[.='Divers']]/td[2]/text()[contains(.,'Terrasse')]").getall())
        if terrace:
            item_loader.add_value("terrace", True)
        item_loader.add_value("landlord_name", "Cabinet Daniel Vêtu")
        item_loader.add_value("landlord_phone", "02 41 25 33 22")
        item_loader.add_value("landlord_email", "vetudaniel@wanadoo.fr")
        yield item_loader.load_item()
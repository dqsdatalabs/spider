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
    name = 'immodurand_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.immodurand.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&ANNLISTEpg=1&tri=d_dt_crea&_=1612848184640",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.immodurand.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=2&lang=fr&ANNLISTEpg=1&tri=d_dt_crea&_=1612848184646",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("&ANNLISTEpg=" + str(page - 1), "&ANNLISTEpg=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immodurand_PySpider_france")

        external_id = response.xpath("//span[contains(.,'Référence de ')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(": ")[1])

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        address = "".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address)
            address = address.split("- ")[1].strip()
            city =address.split("(")[0]
            zipcode = address.split("(")[1].split(")")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//p[contains(@itemprop,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        rent = "".join(response.xpath("//p[contains(.,'Loyer ')]//text()").getall())
        if rent:
            rent = rent.split("Loyer :")[1].split("€")[0].strip().replace(",",".").replace("\xa0","")
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie ')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0]
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0]
            item_loader.add_value("utilities", utilities)
        
        square_meters = response.xpath("//div[contains(@title,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].replace("\xa0","")
            item_loader.add_value("square_meters", square_meters)

        floor = response.xpath("//li/div[contains(.,'Etage')]/following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        room_count = response.xpath("//li/div[contains(.,'Pièces')]/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li/div[contains(.,'Pièce')]/following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li/div[contains(.,'Salle de bain')]/following-sibling::div//text() | //li/div[contains(.,\"Salle d'eau\")]/following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li/div[contains(.,'Parking')]/following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator =response.xpath("//li/div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'oui') or contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        energy_label = response.xpath("//p[contains(.,'Consommations énergétiques')]/parent::div//div[contains(@class,'dpe')][2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        images = [x for x in response.xpath("//a[contains(@class,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Cabinet R.DURAND")
        item_loader.add_value("landlord_phone", "01 47 85 62 62")
        item_loader.add_value("landlord_email", "colombes1958@immodurand.com")

        yield item_loader.load_item()
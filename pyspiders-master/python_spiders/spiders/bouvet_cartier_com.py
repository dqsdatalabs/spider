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
    name = 'bouvet_cartier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bouvet-cartier.com/location/appartement?page=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bouvet-cartier.com/location/maison?page=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//h3/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//a[@title='Page suivante']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bouvet_Cartier_PySpider_france")

        external_id = response.xpath("//strong[contains(.,'Référence :')]/parent::div//strong[contains(@class,'value')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[@class='font-size-xxl']//text()").get()
        if address:
            zipcode = address.split("(")[1].split(")")[0]
            city = address.strip().split(" ")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        desc = " ".join(response.xpath("//div[contains(@class,'mt-40')]//div[contains(@class,'field-item')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        available_date = response.xpath("//div[contains(@class,'mt-40')]//div[contains(@class,'field-item')]//text()[contains(.,'Disponible le') or contains(.,'Disponible')]").get()
        if available_date:
            if not "immédiatement" in available_date:
                available_date = available_date.split("Disponible")[1].replace("le","").replace("à partir du","").replace(" au","").replace(",","").strip()
                if "(ref" in available_date.lower():
                    available_date = available_date.split("(")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        rent = response.xpath("//strong[contains(.,'Loyer')]/parent::div//span//text()").get()
        if rent:
            rent = rent.replace(" ","").split("€")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie')]/parent::div//span//text()").get()
        if deposit:
            deposit = deposit.replace(" ","").split("€")[0]
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//strong[contains(.,'Charges')]/parent::div//span//text()").get()
        if utilities:
            utilities = utilities.replace(" ","").split("€")[0]
            item_loader.add_value("utilities", utilities)

        room_count = response.xpath("//strong[contains(.,'Chambre(s)')]/parent::div//span//text()").get()
        if room_count and room_count.isdigit():
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//strong[contains(.,'Pièce(s)')]/parent::div//span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//strong[contains(.,'Salle(s) de bain')]/parent::div//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//strong[contains(.,'Surface habitable')]/parent::div//span//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters)

        parking = response.xpath("//strong[contains(.,'Parking(s) ')]/parent::div//span//text()").get()
        if parking and parking.isdigit():
            item_loader.add_value("parking", True)

        elevator = response.xpath("//strong[contains(.,'Ascenseur ')]/parent::div//span//text()[not(contains(.,'Non'))]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        balcon_terrace = response.xpath("//strong[contains(.,'Balcon(s) / Terrasse(s)')]/parent::div//span//text()").get()
        if balcon_terrace:
            balcony = balcon_terrace.split("/")[0].strip()
            terrace = balcon_terrace.split("/")[1].strip()
            if balcony!='0':
                item_loader.add_value("balcony", True)
            if terrace!='0':
                item_loader.add_value("terrace", True)

        floor = response.xpath("//strong[contains(.,'Étage')]/parent::div//span//text()").get()
        if floor and floor.isdigit():
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//div[contains(@class,'col text-right')]//img[contains(@class,'diagnostic__image')]//@alt").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            if "none" not in energy_label.lower():
                item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//div[contains(@class,'swiper-slide')]//picture//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude=response.xpath("//meta[@property='og:latitude']/@content").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//meta[@property='og:longitude']/@content").get()
        if longitude:
            item_loader.add_value("longitude",longitude)

        item_loader.add_value("landlord_name", "BOUVET CARTIER IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 50 840 840")
        item_loader.add_value("landlord_email", "agence.annemasse@bouvet-cartier.immo")

        yield item_loader.load_item()
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
    name = 'limmeuble_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.limmeuble.fr/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&ANNLISTEpg=1&tri=d_dt_crea&_=1619704417981",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("&ANNLISTEpg=" + str(page - 1), "&ANNLISTEpg=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Limmeuble_PySpider_france")
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            address = title.split("-")[1].strip()
            city = address.split("(")[0]
            zipcode = title.split("(")[1].split(")")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//span[contains(@itemprop,'price')]//text()").get()
        if rent:
            rent = rent.strip().replace("\u00a0","")
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//li[contains(.,'Charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@itemprop,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        if desc and "dg " in desc.lower():
            deposit = desc.lower().split("dg ")[1]
            if ":" in deposit:
                deposit = deposit.split(":")[1].strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)
            else:
                deposit = deposit.strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)

        square_meters = response.xpath("//li[contains(@title,'Surface')]//div[contains(@class,'bold')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        available_date = response.xpath("//p[@class='dt-dispo'][contains(.,'Disponible le')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1].strip(), date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room_count = response.xpath("//li[contains(@title,'Chambres')]//div[contains(@class,'bold')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(@title,'Pièce')]//div[contains(@class,'bold')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@title,'Salle')]//div[contains(@class,'bold')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        terrace = response.xpath("//li[contains(@title,'Terrasse')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//li[contains(@title,'Balcon')]//div[contains(@class,'bold')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        swimming_pool = response.xpath("//li[contains(@title,'Piscine')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        elevator = response.xpath("//li[contains(@title,'Ascenseur')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(@title,'Etage')]//div[contains(@class,'bold')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        furnished = response.xpath("//li[contains(@title,'Meublé')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        energy_label = response.xpath("//div[contains(@class,'dpe-bloc-lettre')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        images = [x for x in response.xpath("//img[contains(@rel,'gal')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
       
        item_loader.add_value("landlord_name", "L’IMMEUBLE")
        item_loader.add_value("landlord_phone", "05 62 27 84 54")
        item_loader.add_value("landlord_email", "location@limmeuble.fr")
        yield item_loader.load_item()
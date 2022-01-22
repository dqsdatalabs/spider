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
    name = 'soliha_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Soliha_Immobilier_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://soliha-immobilier.fr/liste_annonces.php?FormName=Search&FormAction=search&s_TYPE_ANN=1&s_NATURE_BIEN=2&s_COMMUNE=", 
                "property_type": "apartment"
            },
	        {
                "url": "http://soliha-immobilier.fr/liste_annonces.php?FormName=Search&FormAction=search&s_TYPE_ANN=1&s_NATURE_BIEN=1&s_COMMUNE=", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@class='simple']/li//a[.='Suite']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
        
        next_page = response.xpath("//div/a[font[.='Page suiv.']]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = "".join(response.url)
        if external_id:
            external_id="".join(external_id.split("_")[-1:])
            item_loader.add_value("external_id", external_id.split(".")[0])

        title = response.xpath("//font//b[contains(.,'louer')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//font//text()[contains(.,'Commune ')]").get()
        if address:
            address = address.split(":")[1].strip().replace("- ","")
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        square_meters = response.xpath("//font//text()[contains(.,'Surface habitable')]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//font//text()[contains(.,'Loyer ')]").get()
        if rent:
            rent = rent.split(":")[1].split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//font//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//font//text()[contains(.,'Charges locatives')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//font[contains(.,'Description :')]/following-sibling::font[1]//text()[normalize-space()]").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//font//text()[contains(.,'pièces')]").get()
        if room_count:
            if "chambre" in room_count:
                room_count = room_count.split("chambres")[0].strip().split(" ")[-1]
            else:
                room_count = room_count.split(":")[1].strip()
            item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'thumb-list')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//font//text()[contains(.,'Date de disponibilité')]").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//font[contains(.,'Caractéristiques du bien')]//following-sibling::font//text()[contains(.,'parking') or contains(.,'garage')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//font[contains(.,'Caractéristiques du bien')]//following-sibling::font//text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//font[contains(.,'Caractéristiques du bien')]//following-sibling::font//text()[contains(.,'terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//font//text()[contains(.,'Etage ')]").get()
        if floor:
            floor = floor.split(":")[1].strip()
            if " " in floor:
                floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        item_loader.add_value("landlord_name", "SOLIHA IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 76 85 40 24")
        
        yield item_loader.load_item()
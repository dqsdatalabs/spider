# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser
import re

class MySpider(Spider):
    name = 'adlimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Adlimmo_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.adl-immo.fr/annonces-locations/73-location-appartement.htm?p=1", "property_type": "apartment"},
            {"url": "https://www.adl-immo.fr/annonces-locations/73-location-maison.htm?p=1", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        base_url = response.meta.get("base_url")
        
        seen = False
        for item in response.xpath("//div[@class='description']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = base_url.replace("htm?p=1", f"htm?p={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Adlimmo_PySpider_"+ self.country + "_" + self.locale)
        

        title = response.xpath("//h1[@class='dummy']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = response.xpath("//div[@id='description-bien']/following-sibling::p/text()").get()
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        price = response.xpath("//h2[@class='prix']/text()").get()
        if price:
            rent=price.replace(" ","")
            item_loader.add_value(
                "rent_string", rent)
        
        deposit = response.xpath("normalize-space(//div[@class='caracteristique']//p/span[.='Dépôt de garantie : ']/following-sibling::text()[1])").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value(
                "deposit", str(math.ceil(float(deposit))))

        item_loader.add_xpath(
            "external_id", "normalize-space(//span[@class='reference']/text())"
        )

        square = response.xpath(
            "normalize-space(//div[@class='caracteristique']//ul/li[./span[.='Surface : ']]/text()[3])"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )
        room_count = "".join(response.xpath(
            "//li[span[.='Nombre de chambre(s) : ']]/text()"
        ).getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath(
                "normalize-space(//div[@class='caracteristique']//ul/li[./span[.='Nombre de pièce(s) : ']]/text()[3])"
            ).get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath(
            "normalize-space(//div[@class='caracteristique']//ul/li[./span[contains(.,'de bain')]]/text()[3])"
        ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        elif not bathroom_count:
            bathroom_count = response.xpath("//div[@class='caracteristique']//ul/li[./span[contains(.,'eau') and contains(.,'Nombre de salle')]]/text()[3]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
        city = response.xpath("//div[@id='mapContainer']/meta[@itemprop='addresslocality']/@content").get()
        zipcode = response.xpath("//div[@id='mapContainer']/meta[@itemprop='postalcode']/@content").get()
        item_loader.add_value("address", zipcode + " " + city.lower().capitalize())
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city.lower().capitalize())
            
        available_date = response.xpath(
            "//div[@class='caracteristique']//ul/li[./span[contains(.,'Disponible')]]/text()[3][not(contains(.,'immédiatement'))]").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        floor = "".join(response.xpath("//div[@class='caracteristique']//ul/li[./span[contains(.,'étage') or contains(.,'Etage ') ]]/text()").getall())
        if floor:
            item_loader.add_value(
                "floor", floor.strip().lstrip().rstrip()
            )

        terrace = response.xpath(
            "//ul/li[@class='ann_balconTerasse']/text()[contains(.,'Terrasse')] | //div[@class='caracteristique']//ul/li[./span[contains(.,'Nombre de terrasse(s) : ')]]/text()[3][not(contains(.,'immédiatement'))]"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath(
            "//div[@class='caracteristique']//ul/li[./span[contains(.,'Meublé')]]/text()[3]"
        ).get()
        if furnished:
            if "non" in furnished or "no" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        parking = response.xpath(
            "//div[@class='caracteristique']//ul/li[./span[contains(.,'Nombre de parking(s)')]]/text()[3][not(contains(.,'immédiatement'))] ").get()
        if parking:
            item_loader.add_value("parking", True)

        
        balcony = response.xpath(
            "//div[@class='caracteristique']//ul/li[./span[contains(.,'Nombre de balcon(s)')]]/text()[3]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        
        item_loader.add_xpath("energy_label", "//div[@class='containerStats']/span[@class='dpe']/div[@class='right']/span/@class[not(contains(.,'Z'))]")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='carousel-bien']//a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "05 61 77 27 77")
        item_loader.add_value("landlord_name", "ADL Immobilier")

        lat_long= response.xpath("//div[@class='navigation']/span[@class='back']/a/@href").get()
        if lat_long:
            item_loader.add_value("latitude", lat_long.split("&")[-2].split("=")[1])
            item_loader.add_value("longitude", lat_long.split("&")[-1].split("=")[1])

        yield item_loader.load_item()
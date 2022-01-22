# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'stbenoitimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Stbenoitimmo_PySpider_france"
    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.stbenoitimmo.com/a-louer/appartements/{}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.stbenoitimmo.com/a-louer/maisons-villas/{}",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[@class='card']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)      
        item_loader.add_xpath("title", "//h1/span/text()")
        external_id = response.xpath("//span[@class='labelprix ref']/following-sibling::text()[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        room_count = response.xpath("//tr[th[contains(.,'pièces')]]/th[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//tr[th[contains(.,'pièces')]]/th[2]/text()")
        bathroom_count = response.xpath("//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        zipcode = response.xpath("//tr[th='Code postal']/th[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("//tr[th='Ville']/th[2]/text()").get()
        if address:
            item_loader.add_value("city", address)
            if zipcode:
                address = address+", "+zipcode
            item_loader.add_value("address", address)
   
        item_loader.add_xpath("floor", "//tr[th='Etage']/th[2]/text()")        
        square_meters = response.xpath("//tr[th='Surface habitable (m²)']/th[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split(" m")[0].strip().replace(",","."))))
      
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//tr[th[.='Meublé']]/th[2]/text()[.!='Non renseigné']").get()
        if furnished:
            if furnished.lower() =="non":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr[th[.='Ascenseur']]/th[2]/text()").get()
        if elevator:
            if elevator.lower() =="non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        terrace = response.xpath("//tr[th[.='Terrasse']]/th[2]/text()").get()
        if terrace:
            if terrace.lower() =="non":
                item_loader.add_value("terrace", False)
            elif terrace.lower() =="oui":
                item_loader.add_value("terrace", True)
        balcony = response.xpath("//tr[th[.='Balcon']]/th[2]/text()").get()
        if balcony:
            if balcony.lower() =="non":
                item_loader.add_value("balcony", False)
            elif balcony.lower() =="oui":
                item_loader.add_value("balcony", True)
        parking = response.xpath("//tr[th[.='Nombre de garage' or .='Nombre de parking']]/th[2]/text() ").get()
        if parking:
            if parking.lower() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
        rent = response.xpath("//tr[th[.='Loyer CC* / mois']]/th[2]/text() ").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//tr[th[.='Dépôt de garantie TTC']]/th[2]/text() ").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.replace(" ","").split("€")[0].strip())))
        utilities = response.xpath("//tr[th[contains(.,'Charges locatives')]]/th[2]/text() ").get()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities.replace(" ","").split("€")[0].strip())))
        item_loader.add_value("landlord_name", "SAINT BENOIT IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 67 57 37 37")
        yield item_loader.load_item()
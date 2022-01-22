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
    name = 'lagencebandol_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Lagencebandol_PySpider_france"
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
                "url": [
                    "https://www.lagencebandol.com/a-louer/1",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                print(item)
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@class='Rmenu-list']//li//a[contains(.,'Détails')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_xpath("title", "//div[@class='title-Detail container-section']/h1/text()")

        external_id = "".join(response.xpath("//p[@class='ref']/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        rent = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'Prix')]/span/text()").getall())
        if rent:
            price = rent.replace(",",".").replace(" ","").strip().split("€")[0]
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")

        zipcode = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'Code postal')]//text(),': ')").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        address = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'Ville')]/span/text()").getall())
        if address:
            item_loader.add_value("address", "{} {}".format(address.strip(),zipcode))
            item_loader.add_value("city", address.strip())
        

        meters = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'Surface habitable')]/text()").getall())
        if meters:
            s_meters = meters.split(":")[1].split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters",int(float(s_meters)) )

        room = ""
        room_count = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'chambre')]/text()").getall())
        if room_count:
            room = room_count.split(":")[1].strip()

        else:
            room_count = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'pièces')]/text()").getall())
            if room_count:
                room = room_count.split(":")[1].strip()
        item_loader.add_value("room_count", room.strip())


        description = " ".join(response.xpath("//section[contains(@class,'desciptif-good')]/div/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        images = [ response.urljoin(x) for x in response.xpath("//ul[@class='slide_viewer_X slide_div']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        bathroom_count = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'Nb de salle de bains')]//text(),': ')").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        LatLng = "".join(response.xpath("//script[contains(.,'lat')]/text()").getall())
        if LatLng:
            item_loader.add_xpath("latitude", "substring-before(substring-after(//script[contains(.,'lat')]/text(),'lat : '),',')")
            item_loader.add_xpath("longitude", "substring-before(substring-after(//script[contains(.,'lat')]/text(),'lng:  '),'}')")

        utilities = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'Charges')]/text()[not(contains(.,'Non'))]").getall())
        if utilities:
            uti = utilities.split(":")[1].strip().split("€")[0].replace(",",".").replace(" ","")
            item_loader.add_value("utilities",int(float(uti)))

        deposit = "".join(response.xpath("//ul[contains(@class,'tab-pane')]/li[contains(.,'garantie')]/text()[not(contains(.,'Non'))]").getall())
        if deposit:
            deposit = deposit.split(":")[1].strip().split("€")[0].replace(",",".").replace(" ","")
            item_loader.add_value("deposit",int(float(deposit)))

        elevator = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'Ascenseur ')]//text(),': ')").getall())
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        furnished = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'Meublé ')]//text(),': ')").getall())
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        terrace = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'Terrasse ')]//text(),': ')").getall())
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        parking = "".join(response.xpath("substring-after(//ul[contains(@class,'tab-pane')]/li[contains(.,'parking')]//text(),': ')").getall())
        if parking:
            if parking == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)


        item_loader.add_value("landlord_phone", "04 94 25 03 04")
        item_loader.add_value("landlord_name", "L'AGENCE BANDOL")
        item_loader.add_value("landlord_email", "lagencebandol@yahoo.fr")
        
        yield item_loader.load_item()


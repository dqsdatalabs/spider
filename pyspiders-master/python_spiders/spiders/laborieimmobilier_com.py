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
    name = 'laborieimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    formdata_list = [
        {
            "property_type": "apartment",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "2",
            },
        },
        {
            "property_type": "house",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "1",
            },
        },
        {
            "property_type": "studio",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "4",
            },
        },
    ]

    def start_requests(self):

        yield FormRequest("http://www.laborieimmobilier.com/recherche/",
                        callback=self.parse,
                        formdata=self.formdata_list[0]["formdata"],
                        dont_filter=True,
                        meta={"property_type": self.formdata_list[0]["property_type"], "next_index": 1})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        next_index = response.meta.get("next_index", 1)
        seen = False

        for item in response.xpath("//span[contains(text(),'Voir le bien')]/../@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': "http://www.laborieimmobilier.com/recherche/",
                'Accept-Language': 'tr,en;q=0.9',
            }   
            follow_url = f"http://www.laborieimmobilier.com/recherche/{page}"
            yield Request(follow_url, 
                        headers=headers, 
                        dont_filter=True,
                        callback=self.parse, 
                        meta={"page": page + 1, "property_type": response.meta["property_type"], "next_index": next_index})
                      
        elif len(self.formdata_list) > next_index:
            yield FormRequest("http://www.laborieimmobilier.com/recherche/",
                            callback=self.parse,
                            formdata=self.formdata_list[next_index]["formdata"],
                            dont_filter=True,
                            meta={"property_type": self.formdata_list[next_index]["property_type"], "page": 2, "next_index": next_index + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("com/")[1].split("-")[0])
        item_loader.add_value("external_source", "Laborieimmobilier_PySpider_france")
        item_loader.add_xpath("title", "//title//text()")

        item_loader.add_xpath("external_id", "substring-after(//span[@class='ref']/text(),' ')")
        item_loader.add_xpath("zipcode", "normalize-space(//p[span[.='Code postal']]/span[2]/text())")
        item_loader.add_xpath("city", "normalize-space(//p[span[.='Ville']]/span[2]/text())")
        item_loader.add_value("address","{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))

        item_loader.add_xpath("bathroom_count","normalize-space(//p[span[contains(.,'Nb de salle d')]]/span[2]/text())")
        item_loader.add_xpath("floor","normalize-space(//p[span[contains(.,'Etage')]]/span[2]/text())")
        item_loader.add_xpath("latitude","substring-before(substring-after(//script//text()[contains(.,'center')],'lat: '),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(//script//text()[contains(.,'center')],'lng:  '),'}')")

        square_meters = "".join(response.xpath("substring-before(normalize-space(//p[span[.='Surface habitable (m²)']]/span[2]/text()),' ')").getall())
        if square_meters:
            s_meters = square_meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters)))

        room_count = "".join(response.xpath("//p[span[.='Nombre de chambre(s)']]/span[2]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = "".join(response.xpath("//p[span[.='Nombre de pièces']]/span[2]/text()").getall())
            if room_count:
                item_loader.add_value("room_count", "1")

        rent = "".join(response.xpath("//p[span[contains(.,'Loyer')]]/span[2]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))

        deposit = " ".join(response.xpath("//p[span[contains(.,'Dépôt')]]/span[2]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        utilities = "".join(response.xpath("//p[span[contains(.,'Charges ')]]/span[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        description = " ".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        furnished = "".join(response.xpath("//p[span[.='Meublé']]/span[2]/text()").getall())
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)

        terrace = "".join(response.xpath("//p[span[contains(.,'Terrasse')]]/span[2]/text()").getall())
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        else:
            terrace = "".join(response.xpath("//h1[@itemprop='name']/text()[contains(.,'terrasse')]").getall())
            if terrace:
                item_loader.add_value("terrace", True)

        elevator = "".join(response.xpath("normalize-space(//p[span[contains(.,'Ascenseur')]]/span[2]/text())").getall())
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("normalize-space(//p[span[contains(.,'Balcon')]]/span[2]/text())").getall())
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)


        parking = "".join(response.xpath("normalize-space(//p[span[contains(.,'Balcon')]]/span[2]/text())").getall())
        if parking:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "LABORIE IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 67 88 05 67")
        item_loader.add_value("landlord_email", "laborieimmobilier@orange.fr")

        yield item_loader.load_item()
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
    name = 'orvitis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.orvitis.fr/"
    current_index = 0
    other_prop = ["maison"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "ville": "",
            "type_bien": "appartement",
            "rayon": "0km",
            "op": "Trouver",
            "form_build_id": "form-gn8naQoofJR7VG141UkNS1T4axCYhbJ3TDwE6aMj3cM",
            "form_id": "recherche_orvitis_form_page_location",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//h2/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//li[contains(@class,'pager-next')]/a/@href").get():
            p_url = f"https://www.orvitis.fr/offres-de-location?page={page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "ville": "",
                "type_bien": self.other_prop[self.current_index],
                "rayon": "0km",
                "op": "Trouver",
                "form_build_id": "form-gn8naQoofJR7VG141UkNS1T4axCYhbJ3TDwE6aMj3cM",
                "form_id": "recherche_orvitis_form_page_location",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Orvitis_PySpider_france")      
      
        title = response.xpath("//h1[@id='page-title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if "ascenseur" in title.lower():
                item_loader.add_value("elevator", True)
            if "meublé" in title.lower():
                item_loader.add_value("furnished", True)
   
        item_loader.add_xpath("address", "//div[@class='titreVille']/text()")
        item_loader.add_xpath("city", "//div[@class='titreVille']/text()")

        square_meters = response.xpath("//div[span[.='Surface :']]/text()[normalize-space()]").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].replace(",",".").strip())))
        floor = response.xpath("//div[span[.='Etage :']]/text()[normalize-space()]").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        description = " ".join(response.xpath("//div[@class='field-items']/div/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
            if "chambre" in description:
                room_count = description.split("chambre")[0].strip().split(" ")[-1].strip()
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
        available_date = response.xpath("//div[@class='field-items']/div/p//text()[contains(.,'Disponibilité')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponibilité")[-1].replace("immédiate","now"), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        rent = response.xpath("//table[@class='table-price']//tr[2]/td[1]/text()").get()
        if rent:
            rent = rent.split("€")[0].replace(",",".").replace(" ","")
            item_loader.add_value("rent", int(float(rent.strip())))
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_xpath("utilities", "//table[@class='table-price']//tr[2]/td[2]/text()")
        item_loader.add_xpath("energy_label", "//span[@class='lettre-dpe']/text()")
        longitude = response.xpath("//div[@class='coordonnees-gps']/div[@class='lng']/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        latitude = response.xpath("//div[@class='coordonnees-gps']/div[@class='lat']/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())
        
        landlord_name = response.xpath("//div[@class='agence']/strong/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", 'ORTIVIS')
        landlord_phone = response.xpath("substring-after(//div[@class='agence']//a/@href,':')").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", '0810 021 000')
        yield item_loader.load_item()
# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'cdimmobilier_fr'
    start_urls = ['http://www.cdimmobilier.fr/a-louer/1']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cdimmobilier_PySpider_france_fr'
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='listingUL']/li/article"):
            follow_url = response.urljoin(item.xpath(".//a/@href").extract_first())
            prop_type = item.xpath(".//div[@class='flash-infos']/p[contains(.,'Type de')]/span[2]/text()").extract_first()
            if "Appartement" in prop_type:
                p_type = "apartment"
            elif "Maison" in prop_type:
                p_type = "house"
            else:
                p_type = None
            if p_type:
                yield Request(follow_url, callback=self.populate_item, meta={"prop_type":p_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cdimmobilier_PySpider_"+ self.country + "_" + self.locale)
     
        prop_type = response.meta.get("prop_type")
        
        title = response.xpath("//h1[@itemprop='name']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", prop_type)

        external_id = "".join(response.xpath("//div[@class='col-md-12']/div/div/span[@class='ref']/text()").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.replace("Ref","").strip())

        price = "".join(response.xpath("//div[@class='prix-dt2']/text()[contains(.,'€')]").extract()).strip()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
            # item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("//span[contains(.,'Charges ')]/following-sibling::*/text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(" ",""))

        deposit = "".join(response.xpath("//span[contains(.,'Dépôt ')]/following-sibling::*/text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ",""))

        square = response.xpath("normalize-space(//span[contains(.,'habitable')]/following-sibling::*/text())").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0].strip())


        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        room_count = response.xpath("normalize-space(//span[contains(.,'chambre')]/following-sibling::*/text())").extract_first()
        if room_count:
            item_loader.add_xpath("room_count",room_count)
        else:
            room_count = response.xpath("normalize-space(//span[contains(.,'pièce')]/following-sibling::*/text())").extract_first()
            if room_count:
                item_loader.add_xpath("room_count",room_count)

        item_loader.add_xpath("floor","normalize-space(//span[contains(.,'Etage')]/following-sibling::*/text())")

        desc = "".join(response.xpath("//p[@itemprop='description']/text()").extract())
        item_loader.add_value("description", desc.strip())

        if "DPE :" in desc:
            energy_label=desc.split("DPE :")[1].strip().split(" ")[0]
            if "vierge" not in energy_label and "en" not in energy_label:
                item_loader.add_value("energy_label",energy_label)
        
        balcony = "".join(response.xpath("//span[contains(.,'Balcon')]/following-sibling::*/text()").extract())
        if balcony:
            if "NON" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        terrace = "".join(response.xpath("//span[contains(.,'Terrasse')]/following-sibling::*/text()").extract())
        if terrace:
            if "NON" in terrace:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        parking = "".join(response.xpath("//span[contains(.,'parking') or contains(.,'garage')]/following-sibling::*/text()").extract())
        if parking:
            if "NON" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        elevator = "".join(response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::*/text()").extract())
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False)
            elif "OUI" in elevator:
                item_loader.add_value("elevator", True)

        furnished = "".join(response.xpath("//span[contains(.,'Meublé')]/following-sibling::*/text()").extract())
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False)
            elif "OUI" in furnished.upper():
                item_loader.add_value("furnished", True)

        item_loader.add_xpath("zipcode","normalize-space(//span[contains(.,'Code postal')]/following-sibling::*/text())")

        address = "".join(response.xpath("normalize-space(//div[@class='bienTitle']/h2/text())").extract())
        if address:
            item_loader.add_value("address",address.split("-")[-1].split("(")[0].strip())
            item_loader.add_value("city", address.split("-")[-1].split("(")[0].strip())

        bathroom=response.xpath("//div[@id='details']/p/span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        lat_lng=response.xpath("//script[contains(.,'lat')]/text()").get()
        if lat_lng:
            lat=lat_lng.split("lat :")[1].split(",")[0].strip()
            lng=lat_lng.split("lng:")[1].split("}")[0].strip()
            if lat and lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude",lng)
        
        item_loader.add_value("landlord_phone", "+33 (0) 3 80 49 87 87")
        item_loader.add_value("landlord_email", "contact@cdimmobilier.fr")
        item_loader.add_value("landlord_name", "Cdimmobilier")

        yield item_loader.load_item()
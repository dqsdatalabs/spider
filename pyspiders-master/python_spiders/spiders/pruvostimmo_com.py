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
    name = 'pruvostimmo_com'
    start_urls = ['https://www.pruvost-immo.com/lsi-results/?lsi_s_extends=0&lsi_s_page=1&lsi_s_transaction=rent&lsi_s_search=1']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Pruvostimmo_PySpider_france_fr'
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='an-ad-wrapper lsi-ad-for-list lsi-ad-for-list-photo']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='link-view-details']/@href").extract_first())
            if "position=2" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})
                seen = True
        
        if page == 1 or seen:
            url = f"https://www.pruvost-immo.com/lsi-results/?lsi_s_extends=0&lsi_s_page={page}&lsi_s_transaction=rent&lsi_s_search=1"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='widget-container lsi-widget lsi-widget-ad-title']/h2/span/text()").get()
        if status and "commercial" in status.lower():
            return
        item_loader.add_value("external_source", "Pruvostimmo_PySpider_"+ self.country + "_" + self.locale)

        title = "".join(response.xpath("//h2[@class='widget-title']/span/text()").extract())
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value("title",title.strip())
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = "".join(response.xpath("//p[@class='description']/text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("normalize-space(//div[@id='adtitlelsiwidget-3']//h2[@class='widget-title'][1]/span/text())").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[0].split("-")[-1].strip())
            item_loader.add_value("currency", "EUR")
            
        deposit = response.xpath("//li[contains(@class,'category-conditions')]/ul/li[contains(.,'Dépôt de garantie')]/span/text()").get()
        if deposit:
            item_loader.add_value(
                "deposit", deposit.split("€")[0].strip())

        utilities = response.xpath("//li[contains(@class,'category-conditions')]/ul/li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            item_loader.add_value(
                "utilities", utilities.split("€")[0].strip())
        
        external_id = response.xpath("//p[@class='mandate']/text()").get()
        if external_id:
            item_loader.add_value(
                "external_id", external_id.split(":")[1].strip()
            )

        square = response.xpath(
            "//li[contains(@class,'category-superficies')]/ul/li[./strong[.='Surface habitable']]/span//text()"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split(" ")[0]
            )
       
        room = response.xpath("//li[contains(@class,'category-descriptif')]/ul/li[./strong[.='Nb. de pièces']]/span/text()").get()
        if room:
            item_loader.add_value("room_count", room)

        bathroom = response.xpath("//li[contains(@class,'category-descriptif')]/ul/li[./strong[.='Nb. de salles de bain']]/span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        address = response.xpath("normalize-space(//div[@id='adtextlsiwidget-3']//h2[@class='widget-title']/span/text())").get()
        if address:
            # if "-" in address:
            #     address = address.split("-")[-1]
            item_loader.add_value("address",address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))
            
        
        floor = response.xpath("//li[@class='category category-descriptif isotope-item']/ul/li[./strong[.='Étage du bien']]/span/text()").get()
        if floor:
            item_loader.add_value(
                 "floor", floor.split("er")[0])


        energy = response.xpath("//div[@class='lsi-ad-detail-element dpe']/img[1]/@src").get()
        if energy:
            energy = energy.split("?")[0].split("/")[-1]
            if "VI" not in energy:
                item_loader.add_value("energy_label", energy)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='flexslider carousel']/ul[@class='slides']/li/a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "06 08 24 16 74")
        item_loader.add_value("landlord_name", "GROUPE PRUVOST IMMOBILIER VILLEFRANCHE")
        item_loader.add_value("landlord_email", "agence@pruvost-immo.com")

        yield item_loader.load_item()
def split_address(address, get):

    if "(" in address:
        temp = address.split("(")[1].split(")")[0]
        zip_code = "".join(filter(lambda i: i.isdigit(),temp))
        city = address.split("(")[0].strip()

        if get == "zip":
            return zip_code
        else:
            return city
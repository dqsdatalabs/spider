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
    name = 'portail_immo_net_disabled'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.portail-immo.fr/ajax/ListeBien.php?page=1&RgpdConsent=1614142587294&vlc=4&filtre2=2&lieu-alentour=0&langue=fr",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.portail-immo.fr/ajax/ListeBien.php?page=1&RgpdConsent=1614142587294&vlc=4&filtre8=8&lieu-alentour=0&langue=fr",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        max_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        max_page = int(max_page.split('/')[-1].strip()) if max_page else -1

        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= max_page:
            follow_url = response.url.replace("page=" + str(page - 1), "page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Portail_Immo_PySpider_france")
        item_loader.add_value("external_id", response.url.split("-")[-1].split(".")[0])

        rent = "".join(response.xpath("//div[@class='price-all']/text()").extract())
        if rent:
            price = rent.split("par")[0].replace("\xa0",".").replace(",",".").replace(" ","").replace("€","").strip()
            if price !="NC":
                if int(float(price))>5000:
                    return                    
                else:
                    item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")

        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_xpath("address", "normalize-space(//div[@class='detail-bien-specs']/ul/li[span[.='Ville']]/text())")
        item_loader.add_xpath("city", "normalize-space(//div[@class='detail-bien-specs']/ul/li[span[.='Ville']]/text())")
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@id='Dpe']/img/@src[contains(.,'nrj')],'consommation-'),'.')")


        meters = "".join(response.xpath("//div[@class='detail-bien-specs']/ul/li[span[.='Surface']]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        room_count = "".join(response.xpath("//div[@class='detail-bien-specs']/ul/li[span[.='Chambres']]/text()").extract())
        if room_count:
            if "NC" not in room_count:
                item_loader.add_value("room_count", room_count.strip())
            else:
                item_loader.add_xpath("room_count", "normalize-space(//div[@class='detail-bien-specs']/ul/li[span[.='Pièces']]/text())")

        else:
            item_loader.add_xpath("room_count", "normalize-space(//div[@class='detail-bien-specs']/ul/li[span[.='Pièces']]/text())")

        utilities = "".join(response.xpath("//div[@class='detail-bien-specs']/ul/li/span//span[contains(.,'charges')]/following-sibling::span/text()").extract())
        if utilities:
            utilities= utilities.split("(")[0].replace(",",".").replace("\xa0",".").replace(" ","")
            if utilities !="0":
                item_loader.add_value("utilities", int(float(utilities)))
            else:
                utilities = response.xpath("//div[@class='detail-bien-specs']/ul/li/span//span[contains(.,'Dont h')]/following-sibling::span/text()").get()
                if utilities:
                    item_loader.add_value("utilities", int(float(utilities)))

        deposit = "".join(response.xpath("//div[@class='detail-bien-specs']/ul/li/span//span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").extract())
        if deposit:
            dep = deposit.replace(",",".").replace("\xa0",".").replace(" ","")
            item_loader.add_value("deposit", int(float(dep)))

        desc = " ".join(response.xpath("//div[@class='detail-bien-desc-content']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='thumbs-flap-container']/div/img/@src[not(contains(.,'vide_detail_mini'))]").extract()]
        if images:
                item_loader.add_value("images", images)

        
        latitude = response.xpath("//li[contains(@class,'map-marker-lat')]//text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = response.xpath("//li[contains(@class,'map-marker-lng')]//text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)


        item_loader.add_value("landlord_name", "Portail immo")
        item_loader.add_value("landlord_phone", "04 99 742 942")


        yield item_loader.load_item()
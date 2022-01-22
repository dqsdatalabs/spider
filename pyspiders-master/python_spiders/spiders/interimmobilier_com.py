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

class MySpider(Spider):
    name = 'interimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }    

    def start_requests(self):
        start_urls = [
            {"url": "https://www.inter-immobilier.com/location/1"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        seen = False

        for item in response.xpath("//div[@class='property__content-wrapper']"):

            follow_url = response.urljoin(item.xpath("./a[@class='property__link']/@href").extract_first())
            if "appartement" in follow_url:
                address = item.xpath(".//span[@class='title__subtitle']/text()").extract_first()
                yield Request(follow_url, callback=self.populate_item, meta={"address": address, "property_type": "apartment"})
                seen = True
            elif "duplex" in follow_url:
                address = item.xpath(".//span[@class='title__subtitle']/text()").extract_first()
                yield Request(follow_url, callback=self.populate_item, meta={"address": address, "property_type": "house"})
                seen = True
        
        if seen:
            pagination = response.xpath("normalize-space(//div[@class='pagination__wrapper']/nav/ul/li/a/@href)").extract_first()
            if pagination:
                follow_url = response.urljoin(pagination)
                yield Request(follow_url, callback=self.parse)        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "https://www.inter-immobilier.com/" == response.url:
            return
        
        item_loader.add_value("external_source", "Interimmobilier_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div[contains(@class,'main-info main-info')]//div[@class='title__content']//text()")
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[contains(@class,'detail-1__container-text')]//text()").extract())
        item_loader.add_value("description", desc.strip())

        item_loader.add_xpath("bathroom_count","//div/div[span[contains(.,'salle d')]]/span[2]/text()")
        item_loader.add_xpath("floor","//div/div[span[.='Etage']]/span[2]/text()")
        item_loader.add_xpath("external_id","//div[@class='detail-1__reference']/span/text()")

        item_loader.add_xpath("latitude","//div[@class='detail-1__map']/div/@data-lat")
        item_loader.add_xpath("longitude","//div[@class='detail-1__map']/div/@data-lng")

        price = "".join(response.xpath("//div[@class='main-info__price']/span[1]/text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        room_count = "".join(response.xpath("//div/div[span[.='Nombre de chambre(s)']]/span[2]/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = "".join(response.xpath("//div/div[span[.='Nombre de pièces']]/span[2]/text()").extract())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        utilities = "".join(response.xpath("//div/div[span[contains(.,'Charges locatives')]]/span[2]/text()[contains(.,'€')]").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())

        deposit = "".join(response.xpath("//div[@class='table-aria__tr' and (contains(.,'Dépôt de garantie TTC'))]/span[2]/text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").rstrip("€").strip())

        square = "".join(response.xpath("//div/div[span[.='Surface habitable (m²)']]/span[2]/text()").extract())
        if square:
            square = square.split("m²")[0]
            if "," in square:
                square = square.replace(",",".")
            square = math.ceil(float(square))
            item_loader.add_value("square_meters", str(square))

        item_loader.add_value("property_type", response.meta.get("property_type"))

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'swiper-slide')]/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        terrace = response.xpath("//div/div[span[.='Meublé']]/span[2]/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        terrace = response.xpath("//div/div[span[.='Ascenseur']]/span[2]/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        terrace = response.xpath("//div/div[span[.='Terrasse']]/span[2]/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        address = "".join(response.xpath("//div[@class='col-md-6 titre']/h1/span/text()").extract())
        address = response.meta.get("address")
        item_loader.add_value("address",address)
        item_loader.add_value("city", address.split("(")[0].strip())
        item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])

        item_loader.add_xpath("landlord_phone", "//a[@class='coords-phone__content']/text()")
        item_loader.add_xpath("landlord_email", "//a[@class='coords-mail__content']/text()")
        item_loader.add_value("landlord_name", "Interim Mobilier")

        yield item_loader.load_item()
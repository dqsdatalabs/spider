# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = "immoclairiere"
    
    custom_settings = {
        # "PROXY_ON": True,
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS": 3,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .1,
        "AUTOTHROTTLE_MAX_DELAY": .3,
        "DOWNLOAD_DELAY": 5,
        "HTTPCACHE_ENABLED": False,
        "COOKIES_ENABLED": False, 
    }
    
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = "Immoclairiere_PySpider_belgium_fr"
    def start_requests(self):
        start_urls = [
            {"url": "http://bruxelles.immoclairiere.be/index.php?ctypmandatmeta=l&action=list&cbien=&ctypmetamulti%5B%5D=appt&qchambres=&mprixmin=&mprixmax=", "property_type": "apartment"},
            {"url": "http://bruxelles.immoclairiere.be/index.php?ctypmandatmeta=l&action=list&cbien=&ctypmetamulti%5B%5D=mai&qchambres=&mprixmin=&mprixmax=", "property_type": "house"},
            {"url": "http://braine.immoclairiere.be/index.php?ctypmandatmeta=l&action=list&cbien=&ctypmetamulti%5B%5D=appt&qchambres=&mprixmin=&mprixmax=", "property_type": "apartment"},
            {"url": "http://braine.immoclairiere.be/index.php?ctypmandatmeta=l&action=list&cbien=&ctypmetamulti%5B%5D=mai&qchambres=&mprixmin=&mprixmax=", "property_type": "house"}
        ] 
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='post']"):
            follow_url = response.urljoin(item.xpath(".//h2//@href").get())
            status = item.xpath(".//span[contains(@class,'photolabel')][contains(.,'Lou')]/text()").get()
            if not status:
                yield Request(follow_url,
                                callback=self.populate_item,
                                meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//li[contains(.,'Suivant')]/a/@href").extract_first()
        if pagination:
            yield Request(url=response.urljoin(pagination), 
                             callback=self.parse,
                             meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "".join(response.xpath("//ul[@class='check_list']/li[contains(.,'Loué')]//text()").extract()): return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath(
            "title", "normalize-space(//div[@id='page-title']/h2/text())"
        )
        item_loader.add_xpath("description", "//div[@id='desc']/p//text()")

        price = response.xpath(
            "//div[@id='textbox']/p[1][contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent", price.split("€ ")[0])
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath(
            "//ul[@class='check_list']/li[contains(.,'Précompte:')]"
        ).get()
        if deposit:
            deposit = deposit.split(":")[1]
            item_loader.add_value("deposit", deposit)
        
        ref = response.xpath("//div[@id='textbox']/p[2]").get()
        item_loader.add_value("external_id", ref.split("Réf.:")[1])

        square = response.xpath(
            "//ul[@class='check_list']/li[contains(.,'Surface habitable:')]"
        ).get()
        if square:
            square = square.split(":")[1]
            item_loader.add_value("square_meters", square.split("m²")[0])

        utility = response.xpath(
            "//ul[@class='check_list']/li[contains(.,'Charges:')]"
        ).get()
        if utility:
            utility = utility.split(":")[1]
            item_loader.add_value("utilities", utility.split("€ ")[0])
        
        item_loader.add_xpath( 
            "floor",
            "//div[@class='group']//div[@class='field' and ./div[@class='name' and .='Etage']]/div[@class='value']/text()",
        )

        room = response.xpath(
            "//div[@id='desc']//ul[@class='check_list']/li[contains(.,'Chambre')]/text()"
        ).get()
        if room:
            room = room.split(" ")[0]
            item_loader.add_value("room_count", room)
        
        bath_room = response.xpath("//div[@id='desc']//ul[@class='check_list']/li[contains(.,'Salle de bains')]/text()").get()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room.split(" ")[0])
        elif not bath_room:
            bath=response.xpath('//div[@id="desc"]//ul[@class="check_list"]/li[contains(.,"Salles d")]/text()').get()
            if bath:
                item_loader.add_value("bathroom_count", bath.split(" ")[0])


        terrace = response.xpath("//div[@id='desc']//ul[@class='check_list']/li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//ul[@class='slides']/li/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        
        property_type = response.xpath(
            "//div[@id='desc']/div[@class='headline']/h3/text()"
        ).get()
        item_loader.add_value("address", property_type.split(" - ")[2])
        item_loader.add_value("city", property_type.split(" - ")[2])
        
        item_loader.add_value(
            "property_type", response.meta.get('property_type')
        )
        
        terrace = response.xpath(
            "//div[@class='tabs-container']/div[@id='details']//h4[.='Equipements de Cuisine']"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        terrace = response.xpath(
            "//div[@class='tabs-container']/div[@id='details']//ul/li[contains(.,'Parking')]"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        terrace = response.xpath(
            "//div[@id='details']//ul/li[contains(.,' Ascenseur')]"
        ).get()
        if terrace:
            if terrace is not None:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
                
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Immo Clairière")
        
        phone = response.xpath(
            '//div[@class="five columns"]/ul/li[contains(., "Tél:")]/text()'
        ).get()
        if phone:
            item_loader.add_value(
                "landlord_phone", phone.replace("Tél:", "")
            )
            
        status = response.xpath("//div[@id='textbox']/p[1][contains(., 'Lou')]").get()
        if not status:
            yield item_loader.load_item()

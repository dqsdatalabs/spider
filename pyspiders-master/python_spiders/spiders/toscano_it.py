# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider): 
    name = 'toscano_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Toscano_PySpider_italy"
 
    def start_requests(self):
        start_urls = [
            {"url": "https://www.toscano.it/italia/affitto/ville", "property_type": "house"},
            {"url": "https://www.toscano.it/italia/affitto/appartamenti", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.jump,
                             meta={'property_type': url.get('property_type')})
    def jump(self, response):
        for item in response.xpath("//section[@class='sitemap-agency']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            city = item.xpath("./text()").get()
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta.get('property_type'),"city":city})
    # 1. FOLLOWING
    def parse(self, response):
        listing = response.xpath("//div[following-sibling::div[contains(@id,'MainContent_Immobili_lstVwImmobili_affiniText')]]//div[@class='col-xs-8 description']//a[h2]/@href").getall()
        if not listing:
            listing = response.xpath("//div[@class='col-xs-8 description']/a/@href").getall()
        for item in listing:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"city":response.meta.get('city')})
        if not response.xpath("//div[contains(@id,'MainContent_Immobili_lstVwImmobili_affiniText')]//strong/text()").get():
            next_page = response.xpath("//div/a[span[@id='MainContent_paging_nextSpan']]/@href[.!='javascript:void(0)']").get()
            if next_page:
                yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type'),"city":response.meta.get('city')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response,**kwargs):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("city", response.meta.get('city'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title//text()")

        external_id = response.xpath("//h2[contains(.,'Rif.')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Rif.")[1])
        address=response.xpath("//h2[contains(.,'Rif')]/text()").get()
        if address:
            item_loader.add_value("address",address.split(")")[0].split("(")[-1])

        rent = response.xpath("//span[contains(.,'Euro')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("Euro")[0])
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//span[contains(.,'mq')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("mq")[0])


        room_count = response.xpath("//span[contains(.,'locali')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("locali")[0])

        description = response.xpath("//h3[contains(.,'Descrizione')]//following-sibling::p//text()").get()
        if description:
            item_loader.add_value("description", description)
        latitude=response.xpath("//script[contains(.,'Lat')]/text()").get()
        if latitude:
            latitude=latitude.split("Lat")[-1].split(";")[0]
            if latitude:
                item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'Lat')]/text()").get()
        if longitude:
            longitude=longitude.split("Lng")[-1].split(";")[0]
            if longitude:
                item_loader.add_value("longitude",longitude)

        energy_label = response.xpath("//p[contains(@id,'MainContent_pClasseEnergetica')]//i[contains(@class,'energy')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        utilities = response.xpath("//p[contains(@id,'MainContent_p4')]//text()[contains(.,'€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        terrace = response.xpath("//p[contains(@id,'MainContent_p2')]//text()[contains(.,'Terrazzo')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='image']//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        

        phone=response.xpath("//script[contains(.,'telephone')]/text()").get()
        if phone:
            phone=phone.split('telephone"')[-1].split(",")[0].split(":")[-1].replace('"',"")
            if phone:
                item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_name", "Toscano")

        yield item_loader.load_item()
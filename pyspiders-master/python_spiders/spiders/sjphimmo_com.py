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
    name = 'sjphimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_urls = "https://www.sjphimmo.com/fr/recherche"   # LEVEL 1

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.sjphimmo.com",
        "referer": "https://www.sjphimmo.com/fr/recherche",
    }
    
    formdata = {
        "search-form-29525[search][category]": "Location|2",
        "search-form-29525[search][type]": "",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "type": "Appartement|1",
                "property_type": "apartment"
            },
	        {
                "type": "Maison|2",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            self.formdata["search-form-29525[search][type]"] = url.get('type')
            yield FormRequest(
                url=self.post_urls,
                callback=self.parse,
                formdata=self.formdata,
                headers=self.headers,
                dont_filter=True,
                meta={
                    'property_type': url.get('property_type'),
                }
            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[contains(@class,'property initial')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={
                    'property_type': response.meta.get('property_type'),
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Sjphimmo_PySpider_france")
        
        title = response.xpath("//h2/a/text()").get()
        item_loader.add_value("title", title)
        
        city = response.xpath("//h1/span/text()").get()
        if city:
            item_loader.add_value("address", city)
            item_loader.add_value("city", city)
        
        rent = response.xpath("//p[@class='price']/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace("\u202f","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//li[contains(.,'Pièces')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
            
        external_id = response.xpath("//li[contains(.,'Référence')]/span/text()").get()
        item_loader.add_value("external_id", external_id)
        
        floor = response.xpath("//li[contains(.,'Étage')]/span/text()").get()
        item_loader.add_value("floor", floor)
        
        deposit = response.xpath("//li[contains(.,'Dépôt')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[0])
        
        utilities = response.xpath("//li[contains(.,'sur charges')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0])
        
        description = " ".join(response.xpath("//p[@id='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        washing_machine = response.xpath("//li[contains(.,'Lave-linge')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//li[contains(.,'Lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        images = [x for x in response.xpath("//div[@class='slider']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("landlord_name", "//div[@class='userBubble']//@alt")
        
        phone = response.xpath("//i[contains(@class,'mdi-phone')]/following-sibling::a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("+",""))
        
        item_loader.add_value("landlord_email", "sjphimmo06@orange.fr")
        
        yield item_loader.load_item()
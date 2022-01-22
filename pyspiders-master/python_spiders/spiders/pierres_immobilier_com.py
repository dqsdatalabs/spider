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

class MySpider(Spider):
    name = 'pierres_immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pierres-immobilier.com/annonces-immobilier-1?&criterias[Immoad.type_transaction]=Location&criterias[Immoad.type_bien]=Appartement&criterias[Immoad.prix][0]=0&criterias[Immoad.prix][1]=529000",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='c-theme-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//li[@class='c-next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Pierres_Immobilier_PySpider_france")

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Référence :')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        rent = "".join(response.xpath("normalize-space(//div[@class='c-content-title-1']/div/text())").getall())
        if rent:
            price = rent.replace(" ","").replace("/mois","")
            item_loader.add_value("rent_string", price)

        utilities = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Honoraires :')]/text()[2]").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        deposit = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Dépôt de garantie :')]/text()[2]").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        room_count = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Nombre de chambres :')]/text()[2]").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        square_meters = " ".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Surface habitable :')]/text()[2]").getall()).strip()   
        if square_meters:
            meters =  square_meters.split("m")[0]
            item_loader.add_value("square_meters",int(float(meters)))

        description = " ".join(response.xpath("//div[contains(@class,'rich-text')]/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        images = [ x for x in response.xpath("//div[@class='c-product-gallery-thumbnail']/div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        address = " ".join(response.xpath("//h1/text()[2]").getall()).strip()   
        if address:
            item_loader.add_value("address", address.strip().replace("-",""))
            item_loader.add_value("city", address.strip().split(" ")[1].strip())
            item_loader.add_value("zipcode",address.replace("-","").strip().split(" ")[1].strip())

        available_date=response.xpath("substring-after(//div[@class='c-product-short-desc'][2]/text(),': ')").get()
        if available_date:       
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_value("landlord_phone", "02 32 45 17 75")
        item_loader.add_value("landlord_name", "PIERRES IMMOBILIER")

        yield item_loader.load_item()
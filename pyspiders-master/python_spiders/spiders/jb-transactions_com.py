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
    name = 'jb-transactions_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "JbTransactions_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.jb-transactions.com/immo/c/front/search/pk/1/mode/1/?immo[type]=rent&immo[typeEstate]=-1&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=8&immo[orderOrigin]=inDate&immo[orderWay]=OrderByDescending&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-1",
                    # "https://www.jb-transactions.com/immo/c/front/search/pk/1/mode/1/?immo[type]=rent&immo[typeEstate]=-1&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=1&immo[perPage]=8&immo[orderOrigin]=inDate&immo[orderWay]=OrderByDescending&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-2",
                    # "https://www.jb-transactions.com/immo/c/front/search/pk/1/mode/1/?immo[type]=rent&immo[typeEstate]=-1&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=2&immo[perPage]=8&immo[orderOrigin]=inDate&immo[orderWay]=OrderByDescending&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-3"
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield FormRequest(
                    url=item,
                    callback=self.parse
                )

    # 1. FOLLOWING
    def parse(self, response):
        border=12
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//a[@class='immoListTitleLink']//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if border:
            if page<=int(border)+3:
                if page or seen:
                    nextpage=f"https://www.jb-transactions.com/immo/c/front/search/pk/1/mode/1/?immo[type]=rent&immo[typeEstate]=-1&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]={page}&immo[perPage]=8&immo[orderOrigin]=inDate&immo[orderWay]=OrderByDescending&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-{page+1}"
                    if nextpage:
                        yield Request(nextpage, callback=self.parse,meta={"page":page+1})
            

  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.url
        if dontallow and ("local" in dontallow or "commerce" in dontallow or "garage" in dontallow):
            return

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h3[@class='property-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ","").split("€")[0].strip())
        item_loader.add_value("currency","GBP")
        external_id=response.xpath("//span[@id='immoAttributeRef']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description=response.xpath("//div[@class='property-description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        available_date=response.xpath("//span[contains(.,'Date de l')]/parent::strong/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date.strip())
        square_meters=response.xpath("//span[.='Surface']/parent::strong/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        utilities=response.xpath("//span[.='Honoraires']/parent::strong/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].replace(":","").strip())
        deposit=response.xpath("//span[.='Dépôt de garantie']/parent::strong/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].replace(":","").strip())
        latitude=response.xpath("//script[contains(.,'property.lat')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split('property.lat')[-1].split(";")[0].replace("=",""))
        longitude=response.xpath("//script[contains(.,'property.lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split('property.lng')[-1].split(";")[0].replace("=",""))
        adres=response.xpath("//div[@class='property-location']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//div[@class='property-location']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[0])
        room_count=response.xpath("//span[.='Nb pièce(s)']/parent::strong/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Salle d')]/parent::strong/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=[response.urljoin(x) for x in response.xpath("//a[@rel='lightbox']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","JB TRANSACTIONS")
        yield item_loader.load_item()
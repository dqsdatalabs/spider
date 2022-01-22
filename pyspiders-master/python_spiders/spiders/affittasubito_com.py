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
    name = 'affittasubito_com'
    external_source = "Affittasubito_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['']  # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.affittasubito.com/annunci/?wpp_search%5Bpagination%5D=off&wpp_search%5Bstrict_search%5D=false&wpp_search%5Btipo_annuncio%5D=In+Affitto&wpp_search%5Bproperty_type%5D=appartamento&wpp_search%5Bprice%5D=-1&wpp_search%5Blocali%5D=-1&wpp_search%5Bcomune%5D=-1&wpp_search%5Bagenzia%5D=-1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.affittasubito.com/annunci/?wpp_search%5Bpagination%5D=off&wpp_search%5Bstrict_search%5D=false&wpp_search%5Btipo_annuncio%5D=In+Affitto&wpp_search%5Bproperty_type%5D=loft_mansarde_e_altro&wpp_search%5Bprice%5D=-1&wpp_search%5Blocali%5D=-1&wpp_search%5Bcomune%5D=-1&wpp_search%5Bagenzia%5D=-1",
                    "https://www.affittasubito.com/annunci/?wpp_search%5Bpagination%5D=off&wpp_search%5Bstrict_search%5D=false&wpp_search%5Btipo_annuncio%5D=In+Affitto&wpp_search%5Bproperty_type%5D=villa_indipendente&wpp_search%5Bprice%5D=-1&wpp_search%5Blocali%5D=-1&wpp_search%5Bcomune%5D=-1&wpp_search%5Bagenzia%5D=-1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'property_div property')]"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('?p=')[-1])
        
        title = response.xpath("//h1[@class='property-title entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title.strip())

        room_count = response.xpath("//span[contains(.,'Locali')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        square_meters = response.xpath("//span[contains(.,'Mq')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip())
        else:
            square_meters = response.xpath("//li[contains(@class,'propriet_mq wpp_stat_plain_list_mq')]//span[contains(@class,'value')]//text()").get()
            if square_meters:
                item_loader.add_value("square_meters",square_meters.strip())              

        description = "".join(response.xpath("//div[@class='wpp_the_content']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        else:
            description = "".join(response.xpath("//div[@class='wpp_the_content']/h2/text()").getall())
            if description:
                item_loader.add_value("description",description)
            else:
                description = "".join(response.xpath("//div[@class='wpp_the_content']/h3/text()").getall())
                if description:
                    description = re.sub('\s{2,}', ' ', description.strip())
                    item_loader.add_value("description",description)


        rent = response.xpath("//li[contains(@class,'propriet_price wpp_stat_plain_list_price alt')]//span[contains(@class,'value')]//text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        deposit = "".join(response.xpath("//div[@class='wpp_the_content']/p/text()").getall())
        if deposit and "cauzione" in deposit.lower():
            deposit = deposit.split('cauzione')[-1].split('tre')[-1].split('mensilita')[-1].split("’")[-1].split(',')[0].strip().replace("E","").replace(".","").strip()
            if deposit:
                item_loader.add_value("deposit",deposit)

        address = response.xpath("//li[contains(@class,'propriet_indirizzo wpp_stat_plain_list_indirizzo alt')]//span[contains(@class,'value')]//text()").get()
        if address:
            item_loader.add_value("address",address)
        else:
            address = response.xpath("//li[@class='propriet_indirizzo wpp_stat_plain_list_indirizzo ']/span[2]/text()").get()
            if address:
                item_loader.add_value("address",address)
            else:
                address = response.xpath("//li[@class='propriet_comune wpp_stat_plain_list_comune alt']/span[2]/text()").get()
                if address:
                    item_loader.add_value("address",address)
                else:
                    address = response.xpath("//li[@class='propriet_comune wpp_stat_plain_list_comune ']/span[2]/text()").get()
                    if address:
                        item_loader.add_value("address",address)

        city = response.xpath("//li[contains(@class,'propriet_agenzia wpp_stat_plain_list_agenzia ')]//span[contains(@class,'value')]//text()").get()
        if city:
            item_loader.add_value("city",city)
        else:
            if address:
                if address and "via" in address:
                    city = address.split(',')[1].split(',')[0].strip().split(' ')[1]
                    item_loader.add_value("city", city.strip())
                elif address and "," in address:
                    city = address.split(',')[-1].strip()
                    item_loader.add_value("city", city.strip())
                else:
                    item_loader.add_value("city", address.strip())

        latlng = "".join(response.xpath("//script[@type='text/javascript']/text()").getall())
        if latlng and "latlng" in latlng.lower():
            latitude = latlng.split('LatLng(')[-1].split(',')[0]
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('LatLng(')[-1].split(',')[1].split(')')[0]
            item_loader.add_value("longitude", longitude)

        images = [response.urljoin(x)for x in response.xpath("//dl[contains(@class,'gallery-item')]/dt/a/@href").extract()]
        if images:
                item_loader.add_value("images",images)

        item_loader.add_value("landlord_name", "Affitta Subito")
        item_loader.add_value("landlord_phone", "380 5894303")
        item_loader.add_value("landlord_email", "agenziarimini@affittasubito.com")
        yield item_loader.load_item()
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
    name = '31plusvastgoed_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "31plusvastgoed_PySpider_netherlands"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    formdata={
        "forsaleorrent": "FOR_RENT",
        "moveunavailablelistingstothebottom": "true",
        "take": "10",
        "skip": "0",
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.31plusvastgoed.nl/0-2ac6/aanbod-pagina",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield FormRequest(
                    url=item,
                    formdata=self.formdata,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 10)
        seen = False
        for item in response.xpath("//div[@class='object   ']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 10 or seen:
            headers={
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "cookie": "__RequestVerificationToken=x3u6TuoolY_Zx6Kq7Q7sPlVuVHsM61_3JtHp69VJ9rAyKALmAmhfC3bhuKLmOrreRk3s46VP-FrRT9S0kL6RbyKwxotgpWOm0s_SZQhX23Q1; result_view=gallery; _gcl_au=1.1.1463490238.1639212888; _ga=GA1.2.43551184.1639212895; _gid=GA1.2.294110100.1639212895; _fbp=fb.1.1639212902988.860032394; recently_viewed=1550311,2080283,1593141,1329009,1626740,1863418,1550310; __atuvc=7%7C49; __atuvs=61b4773b36d80edc004; _gat=1; _gat_gtag_UA_111949374_1=1",
                "origin": "https://www.31plusvastgoed.nl",
                "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Mobile Safari/537.36",
                "x-requested-with": "XMLHttpRequest"
            }
            formdata={
                "forsaleorrent": "FOR_RENT",
                "moveunavailablelistingstothebottom": "true",
                "skip": str(page),
                "take": "10",
                
            }
            nextpage="https://www.31plusvastgoed.nl/0-2ac6/aanbod-pagina"
            if nextpage:
                yield FormRequest(nextpage, callback=self.parse,headers=headers,formdata=formdata, meta={"page": page+10,})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        property_type=response.xpath("//td[.='Type object']/following-sibling::td/text()").get()
        if property_type:
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='obj_address']/text()").get()
        if adres:
            item_loader.add_value("address",adres.split(":")[-1].strip())

        rent=response.xpath("//div[@class='object_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split(",")[0].split("-")[0].strip())
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@class='adtext']//text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\t","").replace("\n","").strip())
        images=[x for x in response.xpath("//a[@data-fancybox='listing-photos']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//td[.='Aantal kamers']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//td[.='Aantal badkamers']/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        energy_label=response.xpath("//td[.='Energielabel']/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        square_meters=response.xpath("//td[.='Gebruiksoppervlakte wonen']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        furnished=response.xpath("//td[.='Inrichting']/following-sibling::td/text()").get()
        if furnished and furnished=="Ja":
            item_loader.add_value("furnished",True)
        deposit=response.xpath("//td[.='Borg']/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[1].replace(".","").split(",")[0])
        external_id=response.xpath("//td[.='Referentienummer']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        item_loader.add_value("landlord_name","+31 Vastgoed")
        item_loader.add_value("landlord_email","office@31plusvastgoed.nl")
        
        yield item_loader.load_item()
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
import dateparser

class MySpider(Spider):
    name = 'frances_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    headers = {
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.41 YaBrowser/21.2.0.1097 Yowser/2.5 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': '',
        'Accept-Language': 'tr,en;q=0.9',
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.frances-immobilier.com/ajax/ListeBien.php?menuSave=5&page=1&ListeViewBienForm=text&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=650&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.frances-immobilier.com/ajax/ListeBien.php?menuSave=5&page=1&ListeViewBienForm=text&ope=2&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=650&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                self.headers["Referer"] = "https://www.frances-immobilier.com/fr/liste.htm?" + item.split('?')[1].split('&langue=')[0]
                yield Request(item, callback=self.parse, headers=self.headers, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)
        max_page = int(response.xpath("//span[@class='nav-page-position']/text()").get().split('/')[-1]) if response.xpath("//span[@class='nav-page-position']/text()").get() else -1

        for item in response.xpath("//a[contains(.,'Plus de détails')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        if page <= max_page:
            follow_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            self.headers["Referer"] = "https://www.frances-immobilier.com/fr/liste.htm?" + follow_url.split('?')[1].split('&langue=')[0]
            yield Request(follow_url, callback=self.parse, headers=self.headers, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Frances_Immobilier_PySpider_france")      
        external_id = response.xpath("//div/span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        title = " ".join(response.xpath("//div[@class='leftside-content']//h1/text()").getall())
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title))
        room_count = response.xpath("//li[span[@class='ico-chambre']]/text()[not(contains(.,'NC ch'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("ch")[0])
        else:
            room_count = response.xpath("//li[span[@class='ico-piece']]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])
        address = response.xpath("//h2[@class='detail-bien-ville']//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0].strip())
        square_meters = response.xpath("//li[span[@class='ico-surface']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
      
        description = " ".join(response.xpath("//span[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        available_date = response.xpath("substring-after(//span[@itemprop='description']//text()[contains(.,'Disponible')],'Disponible')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("immédiatement","now"), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [x for x in response.xpath("//div[@class='large-flap-container']//div[@class='diapo is-flap']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        rent = response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[span[contains(.,'Dépôt de garantie')]]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//li/i[span[contains(.,' charges')]]//span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        item_loader.add_xpath("latitude", "//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//li[@class='gg-map-marker-lng']/text()")
        item_loader.add_value("landlord_name", "FRANCES IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 67 662 411")
        item_loader.add_value("landlord_email", "sa.frances@frances-immobilier.com")
        yield item_loader.load_item()
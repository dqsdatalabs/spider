# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = "vbo_nl" 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source = "Vbo_PySpider_netherlands_nl"

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.vbo.nl/huurwoningen?objectsubtype=woonhuis",
                "property_type" : "house"
            },
            {
                "url" : "https://www.vbo.nl/huurwoningen?objectsubtype=appartement",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for item in response.xpath("//div[@class='row']//a[@class='propertyLink']"):

            follow_url = response.urljoin(item.xpath("./@href").get())
            energy = item.xpath(".//figcaption//span[contains(@class,'energielabel')]/text()").get()
            yield Request(follow_url, self.populate_item, meta={"prop_type":response.meta.get("property_type"), "energy" : energy})
        
        next_page = response.xpath("//ul[@class='pagination']//li/a[.='›']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//span[contains(.,'Galerijflat')]/text()").get()
        if status:
            return

        item_loader.add_value("external_source", "Vbo_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("energy_label", response.meta.get("energy"))
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id = response.url
        item_loader.add_value("external_id", external_id.split("woning-")[1].split("-")[0])

        price = response.xpath("//span[@class='price']/text()[contains(., '€')]").extract_first()
        if price:
        #     if "|" in price:
        #         price = price.split("|")[1]
            item_loader.add_value("rent", price.split("€")[-1].split(",")[0])
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//dt[contains(.,'Servicekosten')]/following-sibling::dd[1]/text()[contains(., '€') and .!='€ 0.00 p/m']").extract_first()
        if utilities:
            utilities = utilities.split("€")[1].split(",")[0].replace("p/m","").strip()
            item_loader.add_value("utilities", int(float(utilities)) )

        deposit = response.xpath("//dt[contains(.,'Waarborgsom')]/following-sibling::dd[1]/text()[contains(., '€') and (.!='€ 0.00 p/m' and .!='€ 0.00')]").get()
        if deposit:
            deposit = deposit.split("€")[1].split(".")[0].replace(",","").strip()
            item_loader.add_value("deposit", int(float(deposit)) )
        
        item_loader.add_value("property_type", response.meta.get("prop_type"))

        square = response.xpath("//dt[contains(.,'Woonoppervlakte')]/following-sibling::dd[1]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        images = [response.urljoin(x) for x in response.xpath("//section[@id='gallery']//a[@class='image']/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        room = response.xpath("//dt[contains(.,'Aantal slaapkamers')]/following-sibling::dd[1]/text()").get()
        if room:
            item_loader.add_value("room_count", room)
        else:
            room = response.xpath("//dt[contains(.,'Aantal kamers')]/following-sibling::dd[1]/text()").get()
            if room:
                item_loader.add_value("room_count", room)

        item_loader.add_xpath("bathroom_count", "//dt[contains(.,'Aantal badkamers')]/following-sibling::dd[1]/text()")
        item_loader.add_xpath("floor", "//dt[contains(.,'Aantal woonlagen')]/following-sibling::dd[1]/text()")
        
        date = response.xpath("//dt[contains(.,'Aanvaarding')]/following-sibling::dd[1]/text()").get()
        if date:        
            date_parsed = dateparser.parse(date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        desc = "".join(response.xpath("//div[@id='textDescription']//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc)
     
        parking = response.xpath("//dt[contains(.,'Capaciteit garage')]/following-sibling::dd[1]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
 
 
        city = "".join(response.xpath("//div[@id='summary']//address/p/text()").extract())
        if city:
            item_loader.add_value("city", " ".join(city.strip().split(" ")[1:]))
            item_loader.add_value("zipcode", city.strip().split(" ")[0])
        tit = response.xpath("//h1/text()").extract_first()
        item_loader.add_value("address", " {}, {}".format(tit,city.strip()))

        item_loader.add_xpath("landlord_phone", "//div[@id='contact']//ul[@class='contact']//a[i[@class='icon icon-phone']]/text()[normalize-space()]")
        item_loader.add_xpath("landlord_name", "//div[@id='contact']//a/strong/text()")
        email = response.xpath("//ul[@class='contact']/li/a[contains(@href, 'www')]/@href").get()
        if email:
            item_loader.add_value("landlord_email", f"info@{email.replace('https://www.','')}")
        
        yield item_loader.load_item()


# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = '123wonen_nl' # LEVEL 1
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source='123wonen_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.123wonen.nl/huurwoningen/page/1/"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)

        
        seen = False
        for item in response.xpath("//div[@class='row pandlist']/div[@class='col-xs-12']"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            property_type = item.xpath(".//div[@class='pand-specs']//span[.='Type']/following-sibling::span/text()").get()
            property_type2 = ""
            property_type = property_type.strip()
            if property_type == 'Appartement' or property_type == 'TypeBovenwoning':
                property_type2 = 'apartment'
            elif 'studio' in property_type.lower():
                property_type2 = 'studio'
            elif 'kamer' in property_type.lower():
                property_type2 = 'room'
            elif 'bungalow' in property_type.lower() or property_type == 'TypeHoekwoning' or property_type == 'Eengezinswoning':
                property_type2 = 'house'
            if property_type2:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type2})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.123wonen.nl/huurwoningen/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div/h1/text()")
        item_loader.add_value("external_source", "123wonen_PySpider_"+ self.country + "_" + self.locale)
        
        external_id = response.xpath("//meta[@property='og:url']//@content").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('-', 1)[1])
        address = response.xpath("//h1[@class='panddetail-address-large']/text()").get()
        if address:
            address = address.strip() + ', Netherlands'
            item_loader.add_value("address", address)

        square_meters = response.xpath("//span[.='Woonoppervlakte']/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

            
        room_count = response.xpath("//div[@class='pand-specs']//span[.='Slaapkamers']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('(')[0].strip())
        else:
            room_count = response.xpath("//div[@class='pand-specs']//span[.='Kamers']/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
                
        rent = response.xpath("//span[@class='hidden-xs']/div[@class='panddetail-price']/text()").get()
        if rent:
            rent = rent.split(',')[0].strip()
            item_loader.add_value("rent_string", rent)

        description = response.xpath("//div[@class='contentLayout panddetail-desc']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//h1[@class='panddetail-address-large']/text()").get()
        if city:
            city = city.split(',')[-1].strip()
            item_loader.add_value("city", city)

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//span[.='Beschikbaarheid']/following-sibling::span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('vanaf')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        deposit = response.xpath("//strong[contains(.,'Waarborgsom')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].strip().replace(' ', ''))

        images = [x for x in response.xpath("//div[@id='fotos']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        pets_allowed = response.xpath("//span[.='Huisdieren gewenst']/following-sibling::span/text()").get()
        if pets_allowed:
            if pets_allowed.strip().lower() == 'nee':
                pets_allowed = False
            elif pets_allowed.strip().lower() == 'ja':
                pets_allowed = True
            item_loader.add_value("pets_allowed", pets_allowed)

        parking = response.xpath("//span[.='Parkeerplaats']/following-sibling::span/text()").get()
        if parking:
            if parking.strip().lower() == 'nee':
                parking = False
            elif parking.strip().lower() == 'ja':
                parking = True
            item_loader.add_value("parking", parking)

        furnished = response.xpath("//div[@class='pand-specs']/ul/li/span[contains(.,'Interieur')]//following-sibling::span//text()").get()
        if furnished:
            if "Kaal" in furnished:
                item_loader.add_value("furnished", False)
            elif "Gemeubileerd" in furnished or "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//span[.='Dakterras']/following-sibling::span/text()").get()
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
            elif "nee" in terrace.lower():
                item_loader.add_value("terrace", False)    
        balcony = response.xpath("//span[.='Balkon']/following-sibling::span/text()").get()
        if balcony:
            if balcony.strip().lower() == 'nee':
                balcony = False
            elif balcony.strip().lower() == 'ja':
                balcony = True
            item_loader.add_value("balcony", balcony)
        
        latlng = response.xpath("//script[@type='application/ld+json']//text()[contains(.,'latitude')]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('latitude":"')[1].split('",')[0])
            item_loader.add_value("longitude", latlng.split('longitude":"')[1].split('",')[0])

        item_loader.add_value("landlord_name", "123Wonen")
        landlord_phone = response.xpath("//span[@class='orange' and contains(.,'T')]/following-sibling::text()[1]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//span[@class='orange' and contains(.,'E')]/following-sibling::text()[1]").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        if not item_loader.get_collected_values("deposit"):
            deposit = response.xpath("//text()[contains(.,'De waarborgsom bedraag')]").get()
            if deposit:
                item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split(',')[0].strip())))
            else:
                deposit = response.xpath("//text()[contains(.,'waarborgsom') and contains(.,'maand')]").get()
                if deposit:
                    deposit = "".join(filter(str.isnumeric, deposit.strip()))
                    price = response.xpath("//div[@class='panddetail-price']/text()").get()
                    if price and deposit:
                        price = "".join(filter(str.isnumeric, price.split(',')[0].strip()))
                        deposit = int(deposit) * int(price)
                        item_loader.add_value("deposit", deposit)

        status = response.xpath("//span[contains(@class,'panddetail-status')]//text()").get()
        if status:
            if not "verhuurd" in status.lower() and not "bezichtiging vol" in status.lower():
                yield item_loader.load_item()
        else:
            yield item_loader.load_item()
        
class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class DiamantecasaItSpider(scrapy.Spider):
    name = 'diamantecasa_it'
    allowed_domains = ['diamantecasa.it']
    start_urls = ['https://www.diamantecasa.it/ita/immobili?order_by=date_insert_desc&rental=1&company_id=&seo=&luxury=&categories_id=&price_max=&size_min=&size_max=&rooms=&code=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        urls = response.css('li.pager-pages > a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = "https://www.diamantecasa.it" + urls[i]
        urls.append(self.start_urls)
        for url in urls:
            yield Request(url=url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        for appartment in response.css("#immobili-elenco > div.item"):
            url = appartment.css("a.detail").attrib['href']
            title = appartment.css('div.title > h4::text').get()
            yield Request(url,
                          callback=self.populate_item,
                          meta={'title': title}
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if re.findall(r"(?i)commerciale | (?i)industriale | (?i)ufficio", response.meta['title']):
            return

        external_id = response.url.split('-')[-1]

        description = ''
        description_array = response.css("p.description::text").extract()

        for item in description_array:
            description += item

        feats = response.css('#content2 > div > ul > li')

        space = None
        rent = None
        rooms = None
        floor = None
        parking = None
        energy = None
        elevator = None
        balcony = None
        terrace = None
        for item in feats:
            if "Prezzo" in item.css('span::text').get():
                rent = item.css('b::text').get().split("€")[
                    1].strip().replace('.', "")
            elif "MQ" in item.css('span::text').get():
                space = item.css('b::text').get()
            elif "Locali" in item.css('span::text').get():
                rooms = item.css('b::text').get()
            elif "Piano" in item.css('span::text').get():
                floor = item.css('b::text').get()
            elif "Posti Auto" in item.css('span::text').get():
                parking = True
            elif "Classe Energ." in item.css('span::text').get():
                energy = item.css('b::text').get()
            elif "Ascensore" in item.css('span::text').get():
                elevator = True
            elif "Balcone" in item.css('span::text').get():
                balcony = True
            elif "Terrazzo" in item.css('span::text').get():
                terrace = True

        images = response.css('img.sl::attr(src)').extract()

        lat = None
        lng = None
        zipcode = None
        city = None
        address = None
        try:
            coords = response.xpath(
                '//*[@id="tab-map"]/script/text()').get()
            coords = coords.split('LatLng(')[1].split(")")[0]

            lat = coords.split(',')[0]
            lng = coords.split(',')[1]

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
        except:
            city = 'Catania'


        landlord_name = response.css('h4.nome::text').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", response.meta['title'])
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        # item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("floor", floor)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("energy_label", energy)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", '0957254641')
        item_loader.add_value("landlord_email", 'info@diamantecasa.it')

        yield item_loader.load_item()

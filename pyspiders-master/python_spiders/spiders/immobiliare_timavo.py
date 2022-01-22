import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader

class ImmobiliareTimavoSpider(scrapy.Spider):
    name = 'immo_timavo'
    allowed_domains = ['immobiliaretimavo.com']
    start_urls = ['https://www.immobiliaretimavo.com/immobili/residenziali/in_affitto']
    execution_type = 'development'
    country = 'italy'
    locale ='it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('.z-offer a::attr(href)').extract()
        area_urls = set(area_urls)
        area_urls = ['https://immobiliaretimavo.com/' + x for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        title = response.css("h1::text")[0].extract()
        if "camere" in title or "camera" in title:
            property_type = "room"
        if "Casa" in title:
            property_type = "house"
        if "Garage" in title:
            return
        items = ListingItem()

        external_link = str(response.request.url)
        description = response.css(".z-text::text")[0].extract()
        external_id = response.css(".z-ref span::text")[0].extract()
        external_id = external_id[4:]
        city = response.css("#top h2::text")[0].extract()
        city = city.split("-")[0]

        balcony = False
        furnished = False

        if 'balcone' in description:
            balcony = True
        if 'arredato' in description:
            furnished = True
        if 'non arredato' in description:
            furnished = False

        energy_label = response.css(".mt-2 div:nth-child(1) .semi-bold::text")[0].extract()

        try:
            parking = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(7) > span.z-value::text")[0].extract()
            if parking == "no":
                parking = False
            else:
                parking = True
        except:
            parking = False


        rent = response.css(".z-price span::text")[0].extract()

        square_meters = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(2) > span.z-value::text")[0].extract()
        if any(char.isdigit() for char in square_meters) :
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        floor = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(3) > span.z-value::text")[0].extract()
        floor = floor[0]
        elevator = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(4) > span.z-value::text")[0].extract()
        elevator = elevator.strip()
        if "sì" in elevator:
            elevator = True
        else:
            elevator = False
        room_count = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(5) > span.z-value::text")[0].extract()
        room_count = int(room_count)
        bathroom_count = response.css("#top > div.row.z-specs-row > div.col-12.offset-lg-1.col-lg-4.pl-lg-0.pr-lg-0.z-specs > div:nth-child(6) > span.z-value::text")[0].extract()
        bathroom_count = int(bathroom_count)

        images = response.css('.clearfix img::attr(src)').extract()
        images = ['https://immobiliaretimavo.com/' + x for x in images]

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['city'] = city
        items['balcony'] = balcony
        items['furnished'] = furnished
        items['title'] = title
        items['parking'] = parking
        if energy_label != '-':
            items['energy_label'] = energy_label
        items['description'] = description
        items['property_type'] = property_type
        items['square_meters'] = square_meters
        items['floor'] = floor
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['rent'] = rent
        items['elevator'] = elevator
        items['currency'] = "EUR"
        items['landlord_name'] = "Immobiliare Timavo S.r.l."
        items['landlord_phone'] = "0522 306741"
        items['landlord_email'] = "info@immobiliaretimavo.com"

        items['images'] = images

        yield items
